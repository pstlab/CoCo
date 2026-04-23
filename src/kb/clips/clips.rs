use crate::{
    kb::KnowledgeBaseError,
    model::{Class, Object, Property, Rule, TimedValue, Value},
};
use chrono::{DateTime, Utc};
use clips::{ClipsValue, Environment, Fact, FactBuilder, FactModifier, Type};
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet, VecDeque},
    rc::Rc,
};
use tracing::{error, info, trace};

#[derive(Clone)]
pub struct CLIPSKnowledgeBase {}

struct ActorState {
    classes: HashMap<String, Class>,
    objects: HashMap<String, Object>,
    rules: HashMap<String, Rule>,

    instances: HashMap<String, HashMap<String, Fact>>,               // class name -> object id -> fact
    values: HashMap<String, HashMap<String, HashMap<String, Fact>>>, // class name -> object id -> property name -> fact
}

impl ActorState {
    fn new() -> Self {
        ActorState {
            classes: HashMap::new(),
            objects: HashMap::new(),
            rules: HashMap::new(),
            instances: HashMap::new(),
            values: HashMap::new(),
        }
    }

    fn get_static_properties(&self, classes: HashSet<String>) -> Result<HashMap<String, HashMap<String, Property>>, KnowledgeBaseError> {
        let mut queue: VecDeque<String> = classes.into_iter().collect();
        let mut visited: HashSet<String> = HashSet::new();
        let mut class_properties = HashMap::new();
        while let Some(class_name) = queue.pop_front() {
            if !visited.insert(class_name.clone()) {
                continue;
            }

            let class = self.classes.get(&class_name).ok_or_else(|| KnowledgeBaseError::ClassNotFound(format!("Class {} not found", class_name)))?;
            let mut properties = HashMap::new();
            if let Some(static_props) = &class.static_properties {
                for (name, property) in static_props {
                    properties.entry(name.clone()).or_insert(property.clone());
                }
            }

            if let Some(parents) = &class.parents {
                for parent in parents {
                    if !visited.contains(parent) {
                        queue.push_back(parent.clone());
                    }
                }
            }

            if !properties.is_empty() {
                class_properties.insert(class_name.clone(), properties);
            }
        }

        Ok(class_properties)
    }

    fn get_dynamic_properties(&self, classes: HashSet<String>) -> Result<HashMap<String, HashMap<String, Property>>, KnowledgeBaseError> {
        let mut queue: VecDeque<String> = classes.into_iter().collect();
        let mut visited: HashSet<String> = HashSet::new();
        let mut class_properties = HashMap::new();
        while let Some(class_name) = queue.pop_front() {
            if !visited.insert(class_name.clone()) {
                continue;
            }

            let class = self.classes.get(&class_name).ok_or_else(|| KnowledgeBaseError::ClassNotFound(format!("Class {} not found", class_name)))?;
            let mut properties = HashMap::new();
            if let Some(dynamic_props) = &class.dynamic_properties {
                for (name, property) in dynamic_props {
                    properties.entry(name.clone()).or_insert(property.clone());
                }
            }

            if let Some(parents) = &class.parents {
                for parent in parents {
                    if !visited.contains(parent) {
                        queue.push_back(parent.clone());
                    }
                }
            }

            if !properties.is_empty() {
                class_properties.insert(class_name.clone(), properties);
            }
        }

        Ok(class_properties)
    }

    fn get_object_classes(&self, object: &Object) -> Result<HashSet<String>, KnowledgeBaseError> {
        let mut queue: VecDeque<String> = object.classes.iter().cloned().collect();
        let mut visited: HashSet<String> = HashSet::new();

        while let Some(class_name) = queue.pop_front() {
            if !visited.insert(class_name.clone()) {
                continue;
            }

            let class = self.classes.get(&class_name).ok_or_else(|| KnowledgeBaseError::ClassNotFound(format!("Class {} not found for object {}", class_name, object.id.clone().unwrap_or_default())))?;
            if let Some(parents) = &class.parents {
                for parent in parents {
                    if !visited.contains(parent) {
                        queue.push_back(parent.clone());
                    }
                }
            }
        }

        Ok(visited)
    }

    fn create_class(&mut self, env: &mut Environment, class: Class) -> Result<(), KnowledgeBaseError> {
        if self.classes.contains_key(&class.name) {
            return Err(KnowledgeBaseError::ClassAlreadyExists(class.name));
        }
        env.build(format!("(deftemplate {} (slot id (type SYMBOL)))", class.name).as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create class in CLIPS: {}", e)))?;
        if let Some(static_props) = &class.static_properties {
            for (name, prop) in static_props {
                env.build(prop_deftemplate(&class, name, prop, true).as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create static property {} for class {} in CLIPS: {}", name, class.name, e)))?;
            }
        }
        if let Some(dynamic_props) = &class.dynamic_properties {
            for (name, prop) in dynamic_props {
                env.build(prop_deftemplate(&class, name, prop, false).as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create dynamic property {} for class {} in CLIPS: {}", name, class.name, e)))?;
            }
        }
        self.classes.insert(class.name.clone(), class);
        Ok(())
    }

    fn add_class(&mut self, env: &mut Environment, object_id: &str, class_name: &str) -> Result<(), KnowledgeBaseError> {
        let static_props = self.get_static_properties(HashSet::from([class_name.to_owned()]))?;
        let dynamic_props = self.get_dynamic_properties(HashSet::from([class_name.to_owned()]))?;

        let object = self.objects.get_mut(object_id).ok_or_else(|| KnowledgeBaseError::ObjectNotFound(object_id.to_owned()))?;
        object.classes.insert(class_name.to_owned());

        for (class_name, props) in static_props {
            let class = self.classes.get(&class_name).ok_or_else(|| KnowledgeBaseError::ClassNotFound(format!("Class {} not found for object {}", class_name, object_id)))?;
            for (name, prop) in props {
                let fb = env.fact_builder(&format!("{}_{}", class.name, name)).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact builder for property {} of object {}: {}", name, object_id, e)))?;
                let fb = fb.put_symbol("id", object_id).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set id slot for property {} of object {}: {}", name, object_id, e)))?;
                if let Some(v) = object.properties.as_ref().and_then(|props| props.get(&name)) {
                    let fb: FactBuilder = set_prop(env, fb, &prop, v.clone(), None).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set property {} for object {}: {:#?}", name, object_id, e)))?;
                    let fact = env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for property {} of object {}: {}", name, object_id, e)))?;
                    self.values.entry(class.name.clone()).or_default().entry(object_id.to_owned()).or_default().insert(name.clone(), fact);
                } else {
                    let def = get_default(&prop);
                    let fb = set_prop(env, fb, &prop, def.clone(), None).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set default value for property {} of object {}: {:#?}", name, object_id, e)))?;
                    let fact = env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for default value of property {} of object {}: {}", name, object_id, e)))?;
                    self.values.entry(class.name.clone()).or_default().entry(object_id.to_owned()).or_default().insert(name.clone(), fact);
                }
            }
        }

        for (class_name, props) in dynamic_props {
            let class = self.classes.get(&class_name).ok_or_else(|| KnowledgeBaseError::ClassNotFound(format!("Class {} not found for object {}", class_name, object_id)))?;
            for (name, prop) in props {
                if let Some(v) = object.values.as_ref().and_then(|vals| vals.get(&name)) {
                    let fb = env.fact_builder(&format!("{}_{}", class.name, name)).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact builder for dynamic property {} of object {}: {}", name, object_id, e)))?;
                    let fb = set_prop(env, fb, &prop, v.value.clone(), Some(v.timestamp)).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set dynamic property {} for object {}: {:#?}", name, object_id, e)))?;
                    let fact = env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for dynamic property {} of object {}: {}", name, object_id, e)))?;
                    self.values.entry(class.name.clone()).or_default().entry(object_id.to_owned()).or_default().insert(name.clone(), fact);
                } else {
                    let def = get_default(&prop);
                    let fb = env.fact_builder(&format!("{}_{}", class.name, name)).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact builder for dynamic property {} of object {}: {}", name, object_id, e)))?;
                    let fb = set_prop(env, fb, &prop, def.clone(), Some(Utc::now()))?;
                    let fact = env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for default value of dynamic property {} of object {}: {}", name, object_id, e)))?;
                    self.values.entry(class.name.clone()).or_default().entry(object_id.to_owned()).or_default().insert(name.clone(), fact);
                }
            }
        }

        Ok(())
    }

    fn set_properties(&mut self, env: &mut Environment, object_id: &str, properties: &HashMap<String, Value>) -> Result<(), KnowledgeBaseError> {
        let static_props = self.get_static_properties(self.objects.get(object_id).ok_or_else(|| KnowledgeBaseError::ObjectNotFound(object_id.to_owned()))?.classes.iter().cloned().collect())?;
        let object = self.objects.get_mut(object_id).ok_or_else(|| KnowledgeBaseError::ObjectNotFound(object_id.to_owned()))?;
        for (class_name, props) in static_props {
            for (name, prop) in props {
                if let Some(v) = properties.get(&name) {
                    object.properties.get_or_insert_with(HashMap::new).insert(name.clone(), v.clone());
                    let fact = self.values.get(&class_name).and_then(|objs| objs.get(object_id)).and_then(|props| props.get(&name)).ok_or_else(|| KnowledgeBaseError::ObjectNotFound(format!("Fact for property {} of object {} of class {} not found", name, object_id, class_name)))?;
                    let fm = env.fact_modifier(fact).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact modifier for object {}: {}", object_id, e)))?;
                    let fm = update_prop(env, fm, &prop, v.clone(), None)?;
                    env.modify_fact(fm).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to modify fact for property {} of object {}: {}", name, object_id, e)))?;
                }
            }
        }

        Ok(())
    }

    fn add_values(&mut self, env: &mut Environment, object_id: &str, values: &HashMap<String, Value>, timestamp: DateTime<Utc>) -> Result<(), KnowledgeBaseError> {
        let dynamic_props = self.get_dynamic_properties(self.objects.get(object_id).ok_or_else(|| KnowledgeBaseError::ObjectNotFound(object_id.to_owned()))?.classes.iter().cloned().collect())?;
        let object = self.objects.get_mut(object_id).ok_or_else(|| KnowledgeBaseError::ObjectNotFound(object_id.to_owned()))?;
        for (class_name, props) in dynamic_props {
            for (name, prop) in props {
                if let Some(v) = values.get(&name) {
                    object.values.get_or_insert_with(HashMap::new).insert(name.clone(), TimedValue { value: v.clone(), timestamp });
                    let fact = self.values.get(&class_name).and_then(|objs| objs.get(object_id)).and_then(|props| props.get(&name)).ok_or_else(|| KnowledgeBaseError::ObjectNotFound(format!("Fact for dynamic property {} of object {} of class {} not found", name, object_id, class_name)))?;
                    let fm = env.fact_modifier(fact).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact modifier for object {}: {}", object_id, e)))?;
                    let fm = update_prop(env, fm, &prop, v.clone(), Some(timestamp))?;
                    env.modify_fact(fm).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to modify fact for dynamic property {} of object {}: {}", name, object_id, e)))?;
                }
            }
        }

        Ok(())
    }
}

impl CLIPSKnowledgeBase {
    pub fn new() -> Self {
        info!("Starting CLIPS knowledge base");
        tokio::task::spawn_blocking(move || {
            let mut env = Environment::new().expect("Failed to create CLIPS environment");
            let state = Rc::new(RefCell::new(ActorState::new()));

            let state_add_class = Rc::clone(&state);
            env.add_udf("add-class", None, 2, 2, vec![Type(Type::SYMBOL), Type(Type::SYMBOL)], move |env, ctx| {
                let state = &mut *state_add_class.borrow_mut();
                let object_id = ctx.get_next_argument(Type(Type::SYMBOL)).expect("Failed to get object ID argument for add-class UDF");
                let object_id = if let ClipsValue::Symbol(s) = object_id { s } else { panic!("Expected symbol for object ID argument in add-class UDF") };
                let class_name = ctx.get_next_argument(Type(Type::SYMBOL)).expect("Failed to get class name argument for add-class UDF");
                let class_name = if let ClipsValue::Symbol(s) = class_name { s } else { panic!("Expected symbol for class name argument in add-class UDF") };

                trace!("CLIPS UDF 'add-class' called with object_id='{}' and class_name='{}'", object_id, class_name);
                match state.add_class(env, &object_id, &class_name) {
                    Ok(_) => {
                        trace!("Successfully added class '{}' to object '{}'", class_name, object_id);
                    }
                    Err(e) => {
                        error!("Error adding class '{}' to object '{}': {}", class_name, object_id, e);
                    }
                }

                ClipsValue::Void()
            })
            .expect("Failed to add CLIPS function");

            let state_set_properties = Rc::clone(&state);
            env.add_udf("set-properties", None, 3, 3, vec![Type(Type::SYMBOL), Type(Type::MULTIFIELD), Type(Type::MULTIFIELD)], move |env, ctx| {
                let state = &mut *state_set_properties.borrow_mut();
                let object_id = ctx.get_next_argument(Type(Type::SYMBOL)).expect("Failed to get object ID argument for set-properties UDF");
                let object_id = if let ClipsValue::Symbol(s) = object_id { s } else { panic!("Expected symbol for object ID argument in set-properties UDF") };
                let props = ctx.get_next_argument(Type(Type::MULTIFIELD)).expect("Failed to get properties argument for set-properties UDF");
                let props: Vec<String> = if let ClipsValue::Multifield(mf) = props {
                    mf.into_iter()
                        .map(|v| match v {
                            ClipsValue::Symbol(s) => s,
                            _ => panic!("Expected symbol in properties multifield for set-properties UDF"),
                        })
                        .collect()
                } else {
                    panic!("Expected multifield for properties argument in set-properties UDF");
                };
                let vals = ctx.get_next_argument(Type(Type::MULTIFIELD)).expect("Failed to get values argument for set-properties UDF");
                let vals: Vec<Value> = if let ClipsValue::Multifield(mf) = vals {
                    mf.into_iter()
                        .map(|v| match v {
                            ClipsValue::Integer(i) => Value::Int(i),
                            ClipsValue::Float(f) => Value::Float(f),
                            ClipsValue::Symbol(s) => match s.as_str() {
                                "TRUE" => Value::Bool(true),
                                "FALSE" => Value::Bool(false),
                                "nil" => Value::Null,
                                other => Value::Symbol(other.to_owned()),
                            },
                            ClipsValue::String(s) => Value::String(s),
                            _ => panic!("Expected symbol, integer, or float in values multifield for set-properties UDF"),
                        })
                        .collect()
                } else {
                    panic!("Expected multifield for values argument in set-properties UDF");
                };

                let properties: HashMap<String, Value> = props.into_iter().zip(vals).collect();
                trace!("CLIPS UDF 'set-properties' called with object_id='{}' and properties={:?}", object_id, properties);
                match state.set_properties(env, &object_id, &properties) {
                    Ok(_) => {
                        trace!("Successfully set properties {:?} for object '{}'", properties, object_id);
                    }
                    Err(e) => {
                        error!("Error setting properties {:?} for object '{}': {}", properties, object_id, e);
                    }
                }

                ClipsValue::Void()
            })
            .expect("Failed to add CLIPS function");

            let state_add_data = Rc::clone(&state);
            env.add_udf("add-data", None, 3, 4, vec![Type(Type::SYMBOL), Type(Type::MULTIFIELD), Type(Type::MULTIFIELD), Type(Type::INTEGER)], move |env, ctx| {
                let state = &mut *state_add_data.borrow_mut();
                let object_id = ctx.get_next_argument(Type(Type::SYMBOL)).expect("Failed to get object ID argument for add-data UDF");
                let object_id = if let ClipsValue::Symbol(s) = object_id { s } else { panic!("Expected symbol for object ID argument in add-data UDF") };
                let args = ctx.get_next_argument(Type(Type::MULTIFIELD)).expect("Failed to get args argument for add-data UDF");
                let args: Vec<String> = if let ClipsValue::Multifield(mf) = args {
                    mf.into_iter()
                        .map(|v| match v {
                            ClipsValue::Symbol(s) => s,
                            _ => panic!("Expected symbol, integer, or float in args multifield for add-data UDF"),
                        })
                        .collect()
                } else {
                    panic!("Expected multifield for args argument in add-data UDF");
                };
                let vals = ctx.get_next_argument(Type(Type::MULTIFIELD)).expect("Failed to get values argument for add-data UDF");
                let vals: Vec<Value> = if let ClipsValue::Multifield(mf) = vals {
                    mf.into_iter()
                        .map(|v| match v {
                            ClipsValue::Integer(i) => Value::Int(i),
                            ClipsValue::Float(f) => Value::Float(f),
                            ClipsValue::Symbol(s) => match s.as_str() {
                                "TRUE" => Value::Bool(true),
                                "FALSE" => Value::Bool(false),
                                "nil" => Value::Null,
                                other => Value::Symbol(other.to_owned()),
                            },
                            ClipsValue::String(s) => Value::String(s),
                            _ => panic!("Expected symbol, integer, or float in values multifield for add-data UDF"),
                        })
                        .collect()
                } else {
                    panic!("Expected multifield for values argument in add-data UDF");
                };
                let date_time = if ctx.has_next_argument() { Some(ctx.get_next_argument(Type(Type::INTEGER)).expect("Failed to get date_time argument for add-data UDF")) } else { None };
                let date_time = date_time
                    .map(|dt| {
                        let dt = if let ClipsValue::Integer(i) = dt { i } else { panic!("Expected integer for date_time argument in add-data UDF") };
                        DateTime::<Utc>::from_timestamp(dt, 0).expect("Failed to convert date_time argument in add-data UDF")
                    })
                    .unwrap_or(Utc::now());

                let values: HashMap<String, Value> = args.into_iter().zip(vals).collect();
                trace!("CLIPS UDF 'add-data' called with object_id='{}', values={:?}, and date_time={}", object_id, values, date_time);
                match state.add_values(env, &object_id, &values, date_time) {
                    Ok(_) => {
                        trace!("Successfully added values {:?} to object '{}' at {}", values, object_id, date_time);
                    }
                    Err(e) => {
                        error!("Error adding values {:?} to object '{}' at {}: {}", values, object_id, date_time, e);
                    }
                }

                ClipsValue::Void()
            })
            .expect("Failed to add CLIPS function");
        });

        CLIPSKnowledgeBase {}
    }
}

fn prop_deftemplate(class: &Class, name: &str, property: &Property, is_static: bool) -> String {
    let mut def = format!("(deftemplate {}_{} (slot id (type SYMBOL))", class.name, name);
    match property {
        Property::Bool { default } => {
            def.push_str(" (slot value (type SYMBOL) (allowed-symbols TRUE FALSE nil)");
            if let Some(def_val) = default {
                def.push_str(&format!(" (default {})", if *def_val { "TRUE" } else { "FALSE" }));
            } else {
                def.push_str(" (default nil)");
            }
            def.push(')');
            if !is_static {
                def.push_str(" (slot time (type INTEGER))");
            }
            def.push(')');
            def
        }
        Property::Int { default, min, max } => {
            def.push_str(" (slot value (type INTEGER SYMBOL) (allowed-symbols nil)");
            if let Some(def_val) = default {
                def.push_str(&format!(" (default {})", def_val));
            } else {
                def.push_str(" (default nil)");
            }
            if min.is_some() || max.is_some() {
                let min_str = min.map(|v| v.to_string()).unwrap_or("?VARIABLE".to_owned());
                let max_str = max.map(|v| v.to_string()).unwrap_or("?VARIABLE".to_owned());
                def.push_str(&format!(" (range {} {})", min_str, max_str));
            }
            def.push(')');
            if !is_static {
                def.push_str(" (slot time (type INTEGER))");
            }
            def.push(')');
            def
        }
        Property::Float { default, min, max } => {
            def.push_str(" (slot value (type FLOAT SYMBOL) (allowed-symbols nil)");
            if let Some(def_val) = default {
                let def_str = def_val.to_string();
                let def_str = if def_str.contains('.') { def_str } else { format!("{}.0", def_str) };
                def.push_str(&format!(" (default {})", def_str));
            } else {
                def.push_str(" (default nil)");
            }
            if min.is_some() || max.is_some() {
                let min_str = min
                    .map(|v| {
                        let s = v.to_string();
                        if s.contains('.') { s } else { format!("{}.0", s) }
                    })
                    .unwrap_or("?VARIABLE".to_owned());
                let max_str = max
                    .map(|v| {
                        let s = v.to_string();
                        if s.contains('.') { s } else { format!("{}.0", s) }
                    })
                    .unwrap_or("?VARIABLE".to_owned());
                def.push_str(&format!(" (range {} {})", min_str, max_str));
            }
            def.push(')');
            if !is_static {
                def.push_str(" (slot time (type INTEGER))");
            }
            def.push(')');
            def
        }
        Property::String { default } => {
            def.push_str(" (slot value (type STRING SYMBOL) (allowed-symbols nil)");
            if let Some(def_val) = default {
                def.push_str(&format!(" (default \"{}\")", def_val));
            } else {
                def.push_str(" (default nil)");
            }
            def.push(')');
            if !is_static {
                def.push_str(" (slot time (type INTEGER))");
            }
            def.push(')');
            def
        }
        Property::Symbol { default, allowed_values } => {
            def.push_str(" (slot value (type SYMBOL)");
            if let Some(allowed) = allowed_values {
                def.push_str(" (allowed-symbols nil");
                for v in allowed {
                    def.push_str(&format!(" {}", v));
                }
                def.push(')');
            } else {
                def.push_str(" (allowed-symbols nil)");
            }

            if let Some(def_val) = default {
                def.push_str(&format!(" (default {})", def_val));
            } else {
                def.push_str(" (default nil)");
            }
            def.push(')');
            if !is_static {
                def.push_str(" (slot time (type INTEGER))");
            }
            def.push(')');
            def
        }
        Property::Object { default, .. } => {
            def.push_str(" (slot value (type SYMBOL)");
            if let Some(def_val) = default {
                def.push_str(&format!(" (default {})", def_val));
            } else {
                def.push_str(" (default nil)");
            }
            def.push(')');
            if !is_static {
                def.push_str(" (slot time (type INTEGER))");
            }
            def.push(')');
            def
        }
        Property::BoolArray { default } => {
            def.push_str(" (multislot value (type SYMBOL) (allowed-symbols TRUE FALSE nil)");
            if let Some(def_val) = default {
                let def_str = def_val.iter().map(|b| if *b { "TRUE" } else { "FALSE" }).collect::<Vec<_>>().join(" ");
                def.push_str(&format!(" (default {})", def_str));
            }
            def.push(')');
            if !is_static {
                def.push_str(" (slot time (type INTEGER))");
            }
            def.push(')');
            def
        }
        Property::IntArray { default, min, max } => {
            def.push_str(" (multislot value (type INTEGER SYMBOL) (allowed-symbols nil)");
            if let Some(def_val) = default {
                let def_str = def_val.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(" ");
                def.push_str(&format!(" (default {})", def_str));
            }
            if min.is_some() || max.is_some() {
                let min_str = min.map(|v| v.to_string()).unwrap_or("?VARIABLE".to_owned());
                let max_str = max.map(|v| v.to_string()).unwrap_or("?VARIABLE".to_owned());
                def.push_str(&format!(" (range {} {})", min_str, max_str));
            }
            def.push(')');
            if !is_static {
                def.push_str(" (slot time (type INTEGER))");
            }
            def.push(')');
            def
        }
        Property::FloatArray { default, min, max } => {
            def.push_str(" (multislot value (type FLOAT SYMBOL) (allowed-symbols nil)");
            if let Some(def_val) = default {
                let def_str = def_val
                    .iter()
                    .map(|f| {
                        let s = f.to_string();
                        if s.contains('.') { s } else { format!("{}.0", s) }
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                def.push_str(&format!(" (default {})", def_str));
            }
            if min.is_some() || max.is_some() {
                let min_str = min
                    .map(|v| {
                        let s = v.to_string();
                        if s.contains('.') { s } else { format!("{}.0", s) }
                    })
                    .unwrap_or("?VARIABLE".to_owned());
                let max_str = max
                    .map(|v| {
                        let s = v.to_string();
                        if s.contains('.') { s } else { format!("{}.0", s) }
                    })
                    .unwrap_or("?VARIABLE".to_owned());
                def.push_str(&format!(" (range {} {})", min_str, max_str));
            }
            def.push(')');
            if !is_static {
                def.push_str(" (slot time (type INTEGER))");
            }
            def.push(')');
            def
        }
        Property::StringArray { default } => {
            def.push_str(" (multislot value (type STRING SYMBOL) (allowed-symbols nil)");
            if let Some(def_val) = default {
                let def_str = def_val.iter().map(|s| format!("\"{}\"", s)).collect::<Vec<_>>().join(" ");
                def.push_str(&format!(" (default {})", def_str));
            }
            def.push(')');
            if !is_static {
                def.push_str(" (slot time (type INTEGER))");
            }
            def.push(')');
            def
        }
        Property::SymbolArray { default, allowed_values } => {
            def.push_str(" (multislot value (type SYMBOL)");
            if let Some(allowed) = allowed_values {
                def.push_str(" (allowed-symbols nil");
                for v in allowed {
                    def.push_str(&format!(" {}", v));
                }
                def.push(')');
            } else {
                def.push_str(" (allowed-symbols nil)");
            }
            if let Some(def_val) = default {
                let def_str = def_val.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(" ");
                def.push_str(&format!(" (default {})", def_str));
            }
            def.push(')');
            if !is_static {
                def.push_str(" (slot time (type INTEGER))");
            }
            def.push(')');
            def
        }
        Property::ObjectArray { default, .. } => {
            def.push_str(" (multislot value (type SYMBOL)");
            if let Some(def_val) = default {
                let def_str = def_val.iter().map(|o| o.as_str()).collect::<Vec<_>>().join(" ");
                def.push_str(&format!(" (default {})", def_str));
            } else {
                def.push_str(" (default nil)");
            }
            def.push(')');
            if !is_static {
                def.push_str(" (slot time (type INTEGER))");
            }
            def.push(')');
            def
        }
    }
}

fn set_prop(env: &Environment, fb: FactBuilder, property: &Property, value: Value, time: Option<DateTime<Utc>>) -> Result<FactBuilder, KnowledgeBaseError> {
    let builder = match (property, value) {
        (Property::Bool { .. }, Value::Bool(b)) => fb.put_symbol("value", if b { "TRUE" } else { "FALSE" }).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set bool property value: {}", e))),
        (Property::Bool { .. }, Value::Null) => fb.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for bool property: {}", e))),
        (Property::Int { .. }, Value::Int(i)) => fb.put_int("value", i).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set int property value: {}", e))),
        (Property::Int { .. }, Value::Null) => fb.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for int property: {}", e))),
        (Property::Float { .. }, Value::Float(f)) => fb.put_float("value", f).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set float property value: {}", e))),
        (Property::Float { .. }, Value::Null) => fb.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for float property: {}", e))),
        (Property::String { .. }, Value::String(s)) => fb.put_string("value", s.as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set string property value: {}", e))),
        (Property::String { .. }, Value::Null) => fb.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for string property: {}", e))),
        (Property::Symbol { .. }, Value::Symbol(s)) => fb.put_symbol("value", s.as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set symbol property value: {}", e))),
        (Property::Symbol { .. }, Value::Null) => fb.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for symbol property: {}", e))),
        (Property::Object { .. }, Value::Object(o)) => fb.put_symbol("value", o.as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set object property value: {}", e))),
        (Property::Object { .. }, Value::Null) => fb.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for object property: {}", e))),
        (Property::BoolArray { .. }, Value::BoolArray(arr)) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for bool array: {}", e)))?;
            let builder = arr.iter().fold(builder, |bld, &b| bld.put_symbol(if b { "TRUE" } else { "FALSE" }));
            fb.put_multifield("value", builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set bool array property value: {}", e)))
        }
        (Property::BoolArray { .. }, Value::Null) => fb.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for bool array property: {}", e))),
        (Property::IntArray { .. }, Value::IntArray(arr)) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for int array: {}", e)))?;
            let builder = arr.iter().fold(builder, |bld, &i| bld.put_int(i));
            fb.put_multifield("value", builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set int array property value: {}", e)))
        }
        (Property::IntArray { .. }, Value::Null) => fb.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for int array property: {}", e))),
        (Property::FloatArray { .. }, Value::FloatArray(arr)) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for float array: {}", e)))?;
            let builder = arr.iter().fold(builder, |bld, &f| bld.put_float(f));
            fb.put_multifield("value", builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set float array property value: {}", e)))
        }
        (Property::FloatArray { .. }, Value::Null) => fb.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for float array property: {}", e))),
        (Property::StringArray { .. }, Value::StringArray(arr)) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for string array: {}", e)))?;
            let builder = arr.iter().fold(builder, |bld, s| bld.put_string(s.as_str()));
            fb.put_multifield("value", builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set string array property value: {}", e)))
        }
        (Property::StringArray { .. }, Value::Null) => fb.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for string array property: {}", e))),
        (Property::SymbolArray { .. }, Value::StringArray(arr)) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for symbol array: {}", e)))?;
            let builder = arr.iter().fold(builder, |bld, s| bld.put_symbol(s.as_str()));
            fb.put_multifield("value", builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set symbol array property value: {}", e)))
        }
        (Property::SymbolArray { .. }, Value::Null) => fb.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for symbol array property: {}", e))),
        (Property::ObjectArray { .. }, Value::StringArray(arr)) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for object array: {}", e)))?;
            let builder = arr.iter().fold(builder, |bld, o| bld.put_symbol(o.as_str()));
            fb.put_multifield("value", builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set object array property value: {}", e)))
        }
        (Property::ObjectArray { .. }, Value::Null) => fb.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for object array property: {}", e))),
        _ => Err(KnowledgeBaseError::KBError("Property type and value type do not match".to_owned())),
    };
    if let Some(t) = time { builder.and_then(|fb| fb.put_int("time", t.timestamp()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set time slot for property value: {}", e)))) } else { builder }
}

fn update_prop(env: &Environment, fm: FactModifier, property: &Property, value: Value, time: Option<DateTime<Utc>>) -> Result<FactModifier, KnowledgeBaseError> {
    let modifier = match (property, value) {
        (Property::Bool { .. }, Value::Bool(b)) => fm.put_symbol("value", if b { "TRUE" } else { "FALSE" }).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set bool property value: {}", e))),
        (Property::Bool { .. }, Value::Null) => fm.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for bool property: {}", e))),
        (Property::Int { .. }, Value::Int(i)) => fm.put_int("value", i).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set int property value: {}", e))),
        (Property::Int { .. }, Value::Null) => fm.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for int property: {}", e))),
        (Property::Float { .. }, Value::Float(f)) => fm.put_float("value", f).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set float property value: {}", e))),
        (Property::Float { .. }, Value::Null) => fm.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for float property: {}", e))),
        (Property::String { .. }, Value::String(s)) => fm.put_string("value", s.as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set string property value: {}", e))),
        (Property::String { .. }, Value::Null) => fm.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for string property: {}", e))),
        (Property::Symbol { .. }, Value::Symbol(s)) => fm.put_symbol("value", s.as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set symbol property value: {}", e))),
        (Property::Symbol { .. }, Value::Null) => fm.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for symbol property: {}", e))),
        (Property::Object { .. }, Value::Object(o)) => fm.put_symbol("value", o.as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set object property value: {}", e))),
        (Property::Object { .. }, Value::Null) => fm.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for object property: {}", e))),
        (Property::BoolArray { .. }, Value::BoolArray(arr)) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for bool array: {}", e)))?;
            let builder = arr.iter().fold(builder, |bld, &b| bld.put_symbol(if b { "TRUE" } else { "FALSE" }));
            fm.put_multifield("value", builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set bool array property value: {}", e)))
        }
        (Property::BoolArray { .. }, Value::Null) => fm.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for bool array property: {}", e))),
        (Property::IntArray { .. }, Value::IntArray(arr)) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for int array: {}", e)))?;
            let builder = arr.iter().fold(builder, |bld, &i| bld.put_int(i));
            fm.put_multifield("value", builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set int array property value: {}", e)))
        }
        (Property::IntArray { .. }, Value::Null) => fm.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for int array property: {}", e))),
        (Property::FloatArray { .. }, Value::FloatArray(arr)) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for float array: {}", e)))?;
            let builder = arr.iter().fold(builder, |bld, &f| bld.put_float(f));
            fm.put_multifield("value", builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set float array property value: {}", e)))
        }
        (Property::FloatArray { .. }, Value::Null) => fm.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for float array property: {}", e))),
        (Property::StringArray { .. }, Value::StringArray(arr)) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for string array: {}", e)))?;
            let builder = arr.iter().fold(builder, |bld, s| bld.put_string(s.as_str()));
            fm.put_multifield("value", builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set string array property value: {}", e)))
        }
        (Property::StringArray { .. }, Value::Null) => fm.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for string array property: {}", e))),
        (Property::SymbolArray { .. }, Value::StringArray(arr)) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for symbol array: {}", e)))?;
            let builder = arr.iter().fold(builder, |bld, s| bld.put_symbol(s.as_str()));
            fm.put_multifield("value", builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set symbol array property value: {}", e)))
        }
        (Property::SymbolArray { .. }, Value::Null) => fm.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for symbol array property: {}", e))),
        (Property::ObjectArray { .. }, Value::StringArray(arr)) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for object array: {}", e)))?;
            let builder = arr.iter().fold(builder, |bld, o| bld.put_symbol(o.as_str()));
            fm.put_multifield("value", builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set object array property value: {}", e)))
        }
        (Property::ObjectArray { .. }, Value::Null) => fm.put_symbol("value", "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null value for object array property: {}", e))),
        _ => Err(KnowledgeBaseError::KBError("Property type and value type do not match".to_owned())),
    };
    if let Some(t) = time { modifier.and_then(|fm| fm.put_int("time", t.timestamp()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set time slot for property value: {}", e)))) } else { modifier }
}

fn set_value(env: &Environment, fb: FactBuilder, slot: &str, value: &Value) -> Result<FactBuilder, KnowledgeBaseError> {
    match value {
        Value::Bool(b) => fb.put_symbol(slot, if *b { "TRUE" } else { "FALSE" }).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set bool field {}: {}", slot, e))),
        Value::Int(i) => fb.put_int(slot, *i).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set int field {}: {}", slot, e))),
        Value::Float(f) => fb.put_float(slot, *f).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set float field {}: {}", slot, e))),
        Value::String(s) => fb.put_string(slot, s.as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set string field {}: {}", slot, e))),
        Value::Symbol(s) | Value::Object(s) => fb.put_symbol(slot, s.as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set symbol field {}: {}", slot, e))),
        Value::Null => fb.put_symbol(slot, "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null field {}: {}", slot, e))),
        Value::BoolArray(arr) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for {}: {}", slot, e)))?;
            let builder = arr.iter().fold(builder, |b, &v| b.put_symbol(if v { "TRUE" } else { "FALSE" }));
            fb.put_multifield(slot, builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set bool array field {}: {}", slot, e)))
        }
        Value::IntArray(arr) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for {}: {}", slot, e)))?;
            let builder = arr.iter().fold(builder, |b, &v| b.put_int(v));
            fb.put_multifield(slot, builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set int array field {}: {}", slot, e)))
        }
        Value::FloatArray(arr) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for {}: {}", slot, e)))?;
            let builder = arr.iter().fold(builder, |b, &v| b.put_float(v));
            fb.put_multifield(slot, builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set float array field {}: {}", slot, e)))
        }
        Value::StringArray(arr) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for {}: {}", slot, e)))?;
            let builder = arr.iter().fold(builder, |b, v| b.put_string(v.as_str()));
            fb.put_multifield(slot, builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set string array field {}: {}", slot, e)))
        }
        Value::SymbolArray(arr) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for {}: {}", slot, e)))?;
            let builder = arr.iter().fold(builder, |b, v| b.put_string(v.as_str()));
            fb.put_multifield(slot, builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set string array field {}: {}", slot, e)))
        }
        Value::ObjectArray(arr) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for {}: {}", slot, e)))?;
            let builder = arr.iter().fold(builder, |b, v| b.put_string(v.as_str()));
            fb.put_multifield(slot, builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set string array field {}: {}", slot, e)))
        }
    }
}

fn update_value(env: &Environment, fm: FactModifier, slot: &str, value: &Value) -> Result<FactModifier, KnowledgeBaseError> {
    match value {
        Value::Bool(b) => fm.put_symbol(slot, if *b { "TRUE" } else { "FALSE" }).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set bool field {}: {}", slot, e))),
        Value::Int(i) => fm.put_int(slot, *i).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set int field {}: {}", slot, e))),
        Value::Float(f) => fm.put_float(slot, *f).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set float field {}: {}", slot, e))),
        Value::String(s) => fm.put_string(slot, s.as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set string field {}: {}", slot, e))),
        Value::Symbol(s) | Value::Object(s) => fm.put_symbol(slot, s.as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set symbol field {}: {}", slot, e))),
        Value::Null => fm.put_symbol(slot, "nil").map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set null field {}: {}", slot, e))),
        Value::BoolArray(arr) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for {}: {}", slot, e)))?;
            let builder = arr.iter().fold(builder, |b, &v| b.put_symbol(if v { "TRUE" } else { "FALSE" }));
            fm.put_multifield(slot, builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set bool array field {}: {}", slot, e)))
        }
        Value::IntArray(arr) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for {}: {}", slot, e)))?;
            let builder = arr.iter().fold(builder, |b, &v| b.put_int(v));
            fm.put_multifield(slot, builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set int array field {}: {}", slot, e)))
        }
        Value::FloatArray(arr) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for {}: {}", slot, e)))?;
            let builder = arr.iter().fold(builder, |b, &v| b.put_float(v));
            fm.put_multifield(slot, builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set float array field {}: {}", slot, e)))
        }
        Value::StringArray(arr) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for {}: {}", slot, e)))?;
            let builder = arr.iter().fold(builder, |b, v| b.put_string(v.as_str()));
            fm.put_multifield(slot, builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set string array field {}: {}", slot, e)))
        }
        Value::SymbolArray(arr) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for {}: {}", slot, e)))?;
            let builder = arr.iter().fold(builder, |b, v| b.put_string(v.as_str()));
            fm.put_multifield(slot, builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set string array field {}: {}", slot, e)))
        }
        Value::ObjectArray(arr) => {
            let builder = env.multifield_builder(arr.len()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create multifield for {}: {}", slot, e)))?;
            let builder = arr.iter().fold(builder, |b, v| b.put_string(v.as_str()));
            fm.put_multifield(slot, builder.create()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set string array field {}: {}", slot, e)))
        }
    }
}

fn get_default(property: &Property) -> Value {
    match property {
        Property::Bool { default, .. } => default.map(Value::Bool).unwrap_or(Value::Null),
        Property::Int { default, .. } => default.map(Value::Int).unwrap_or(Value::Null),
        Property::Float { default, .. } => default.map(Value::Float).unwrap_or(Value::Null),
        Property::String { default, .. } => default.clone().map(Value::String).unwrap_or(Value::Null),
        Property::Symbol { default, .. } => default.clone().map(Value::Symbol).unwrap_or(Value::Null),
        Property::Object { default, .. } => default.clone().map(Value::Object).unwrap_or(Value::Null),
        Property::BoolArray { default } => default.clone().map(Value::BoolArray).unwrap_or(Value::Null),
        Property::IntArray { default, .. } => default.clone().map(Value::IntArray).unwrap_or(Value::Null),
        Property::FloatArray { default, .. } => default.clone().map(Value::FloatArray).unwrap_or(Value::Null),
        Property::StringArray { default } => default.clone().map(Value::StringArray).unwrap_or(Value::Null),
        Property::SymbolArray { default, .. } => default.clone().map(Value::StringArray).unwrap_or(Value::Null),
        Property::ObjectArray { default, .. } => default.clone().map(Value::StringArray).unwrap_or(Value::Null),
    }
}
