use crate::{
    kb::{KnowledgeBase, KnowledgeBaseError, KnowledgeBaseEvent},
    model::{Class, Object, Property, Rule, TimedValue, Value},
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clips::{ClipsValue, Environment, Fact, FactBuilder, FactModifier, Type, UDFContext};
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet, VecDeque},
    rc::Rc,
};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, trace};

type Udf = Box<dyn FnMut(&mut Environment, &mut UDFContext) -> ClipsValue + Send>;
type ClassPropertyMap = HashMap<String, HashMap<String, Property>>;

enum KBCommand {
    CreateClass(Class, oneshot::Sender<Result<(), KnowledgeBaseError>>),
    CreateRule(Rule, oneshot::Sender<Result<(), KnowledgeBaseError>>),
    CreateObject(Object, oneshot::Sender<Result<(), KnowledgeBaseError>>),
    GetStaticProperties(HashSet<String>, oneshot::Sender<Result<ClassPropertyMap, KnowledgeBaseError>>),
    GetDynamicProperties(HashSet<String>, oneshot::Sender<Result<ClassPropertyMap, KnowledgeBaseError>>),
    AddClass(String, String, oneshot::Sender<Result<(), KnowledgeBaseError>>),
    GetObjectClasses(String, oneshot::Sender<Result<HashSet<String>, KnowledgeBaseError>>),
    SetProperties(String, HashMap<String, Value>, oneshot::Sender<Result<(), KnowledgeBaseError>>),
    AddValues(String, HashMap<String, Value>, DateTime<Utc>, oneshot::Sender<Result<(), KnowledgeBaseError>>),
    AddUDF(String, Option<Type>, u16, u16, Vec<Type>, Udf, oneshot::Sender<Result<(), KnowledgeBaseError>>),
}

#[derive(Clone)]
pub struct CLIPSKnowledgeBase {
    command_tx: mpsc::UnboundedSender<KBCommand>,
}

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

    fn create_rule(&mut self, env: &mut Environment, rule: Rule) -> Result<(), KnowledgeBaseError> {
        if self.rules.contains_key(&rule.name) {
            return Err(KnowledgeBaseError::RuleAlreadyExists(rule.name.clone()));
        }

        env.build(rule.content.as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create rule in CLIPS: {}", e)))?;
        self.rules.insert(rule.name.clone(), rule);

        Ok(())
    }

    fn create_object(&mut self, env: &mut Environment, object: Object) -> Result<(), KnowledgeBaseError> {
        let object_id = object.id.clone().ok_or(KnowledgeBaseError::ObjectIDRequired)?;
        if self.objects.contains_key(&object_id) {
            return Err(KnowledgeBaseError::ObjectAlreadyExists(object_id));
        }

        let classes = self.get_object_classes(&object)?;
        if classes.is_empty() {
            return Err(KnowledgeBaseError::ObjectClassesRequired(object_id));
        }
        for class_name in classes {
            if !self.classes.contains_key(&class_name) {
                return Err(KnowledgeBaseError::ClassNotFound(format!("Class {} not found for object {}", class_name, object_id)));
            }
            let fb = env.fact_builder(&class_name).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact builder for class {}: {}", class_name, e)))?;
            let fb = fb.put_symbol("id", object_id.as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set id slot for object {}: {}", object_id, e)))?;
            let fact = env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for object {}: {}", object_id, e)))?;
            self.instances.entry(class_name).or_default().insert(object_id.clone(), fact);
        }

        for (class_name, props) in self.get_static_properties(object.classes.iter().cloned().collect())? {
            for (name, prop) in props {
                let template_name = format!("{}_{}", class_name, name);
                let fb = env.fact_builder(&template_name).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact builder for property {} of object {}: {}", name, object_id, e)))?;
                let fb = fb.put_symbol("id", object_id.as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set id slot for property {} of object {}: {}", name, object_id, e)))?;
                if let Some(v) = object.properties.as_ref().and_then(|props| props.get(&name)) {
                    let fb: FactBuilder = set_prop(env, fb, &prop, v.clone(), None).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set property {} for object {}: {:#?}", name, object_id, e)))?;
                    let fact = env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for property {} of object {}: {}", name, object_id, e)))?;
                    self.values.entry(class_name.clone()).or_default().entry(object_id.clone()).or_default().insert(name.clone(), fact);
                } else {
                    let def = get_default(&prop);
                    let fb = set_prop(env, fb, &prop, def.clone(), None).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set default value for property {} of object {}: {:#?}", name, object_id, e)))?;
                    let fact = env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for default value of property {} of object {}: {}", name, object_id, e)))?;
                    self.values.entry(class_name.clone()).or_default().entry(object_id.clone()).or_default().insert(name.clone(), fact);
                }
            }
        }

        for (class_name, props) in self.get_dynamic_properties(object.classes.iter().cloned().collect())? {
            for (name, prop) in props {
                let template_name = format!("{}_{}", class_name, name);
                let fb = env.fact_builder(&template_name).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact builder for dynamic property {} of object {}: {}", name, object_id, e)))?;
                let fb = fb.put_symbol("id", object_id.as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set id slot for dynamic property {} of object {}: {}", name, object_id, e)))?;
                if let Some(v) = object.values.as_ref().and_then(|vals| vals.get(&name)) {
                    let fb = set_prop(env, fb, &prop, v.value.clone(), Some(v.timestamp)).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set dynamic property {} for object {}: {:#?}", name, object_id, e)))?;
                    let fact = env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for dynamic property {} of object {}: {}", name, object_id, e)))?;
                    self.values.entry(class_name.clone()).or_default().entry(object_id.clone()).or_default().insert(name.clone(), fact);
                } else {
                    let def = get_default(&prop);
                    let fb = set_prop(env, fb, &prop, def.clone(), Some(Utc::now())).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set default value for dynamic property {} of object {}: {:#?}", name, object_id, e)))?;
                    let fact = env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for default value of dynamic property {} of object {}: {}", name, object_id, e)))?;
                    self.values.entry(class_name.clone()).or_default().entry(object_id.clone()).or_default().insert(name.clone(), fact);
                }
            }
        }

        self.objects.insert(object_id.clone(), object);

        Ok(())
    }

    fn add_class(&mut self, env: &mut Environment, object_id: &str, class_name: &str) -> Result<(), KnowledgeBaseError> {
        let static_props = self.get_static_properties(HashSet::from([class_name.to_owned()]))?;
        let dynamic_props = self.get_dynamic_properties(HashSet::from([class_name.to_owned()]))?;

        let object = self.objects.get_mut(object_id).ok_or_else(|| KnowledgeBaseError::ObjectNotFound(object_id.to_owned()))?;
        object.classes.insert(class_name.to_owned());

        for (class_name, props) in static_props {
            for (name, prop) in props {
                let fb = env.fact_builder(&format!("{}_{}", class_name, name)).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact builder for property {} of object {}: {}", name, object_id, e)))?;
                let fb = fb.put_symbol("id", object_id).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set id slot for property {} of object {}: {}", name, object_id, e)))?;
                if let Some(v) = object.properties.as_ref().and_then(|props| props.get(&name)) {
                    let fb: FactBuilder = set_prop(env, fb, &prop, v.clone(), None).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set property {} for object {}: {:#?}", name, object_id, e)))?;
                    let fact = env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for property {} of object {}: {}", name, object_id, e)))?;
                    self.values.entry(class_name.clone()).or_default().entry(object_id.to_owned()).or_default().insert(name.clone(), fact);
                } else {
                    let def = get_default(&prop);
                    let fb = set_prop(env, fb, &prop, def.clone(), None).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set default value for property {} of object {}: {:#?}", name, object_id, e)))?;
                    let fact = env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for default value of property {} of object {}: {}", name, object_id, e)))?;
                    self.values.entry(class_name.clone()).or_default().entry(object_id.to_owned()).or_default().insert(name.clone(), fact);
                }
            }
        }

        for (class_name, props) in dynamic_props {
            for (name, prop) in props {
                if let Some(v) = object.values.as_ref().and_then(|vals| vals.get(&name)) {
                    let fb = env.fact_builder(&format!("{}_{}", class_name, name)).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact builder for dynamic property {} of object {}: {}", name, object_id, e)))?;
                    let fb = set_prop(env, fb, &prop, v.value.clone(), Some(v.timestamp)).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set dynamic property {} for object {}: {:#?}", name, object_id, e)))?;
                    let fact = env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for dynamic property {} of object {}: {}", name, object_id, e)))?;
                    self.values.entry(class_name.clone()).or_default().entry(object_id.to_owned()).or_default().insert(name.clone(), fact);
                } else {
                    let def = get_default(&prop);
                    let fb = env.fact_builder(&format!("{}_{}", class_name, name)).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact builder for dynamic property {} of object {}: {}", name, object_id, e)))?;
                    let fb = set_prop(env, fb, &prop, def.clone(), Some(Utc::now()))?;
                    let fact = env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for default value of dynamic property {} of object {}: {}", name, object_id, e)))?;
                    self.values.entry(class_name.clone()).or_default().entry(object_id.to_owned()).or_default().insert(name.clone(), fact);
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
    pub fn new() -> (Self, mpsc::UnboundedReceiver<KnowledgeBaseEvent>) {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        info!("Starting CLIPS knowledge base");
        tokio::task::spawn_blocking(move || {
            let mut env = Environment::new().expect("Failed to create CLIPS environment");
            let state = Rc::new(RefCell::new(ActorState::new()));

            let state_add_class = Rc::clone(&state);
            let event_tx_add_class = event_tx.clone();
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
                        let _ = event_tx_add_class.send(KnowledgeBaseEvent::AddedClass(object_id.clone(), class_name.clone()));
                    }
                    Err(e) => {
                        error!("Error adding class '{}' to object '{}': {}", class_name, object_id, e);
                    }
                }

                ClipsValue::Void()
            })
            .expect("Failed to add CLIPS function");

            let state_set_properties = Rc::clone(&state);
            let event_tx_set_properties = event_tx.clone();
            env.add_udf("set-properties", None, 3, 3, vec![Type(Type::SYMBOL), Type(Type::MULTIFIELD), Type(Type::MULTIFIELD)], move |env, ctx| {
                let state = &mut *state_set_properties.borrow_mut();
                let object_id = match ctx.get_next_argument(Type(Type::SYMBOL)).expect("Failed to get object ID argument for prompt UDF") {
                    ClipsValue::Symbol(s) => s.to_string(),
                    _ => panic!("Expected symbol for object ID argument in prompt UDF"),
                };
                let props: Vec<String> = match ctx.get_next_argument(Type(Type::MULTIFIELD)).expect("Failed to get properties argument for set-properties UDF") {
                    ClipsValue::Multifield(mf) => mf
                        .into_iter()
                        .map(|v| match v {
                            ClipsValue::Symbol(s) => s,
                            _ => panic!("Expected symbol in properties multifield for set-properties UDF"),
                        })
                        .collect(),
                    _ => panic!("Expected multifield for properties argument in set-properties UDF"),
                };
                let vals: Vec<Value> = match ctx.get_next_argument(Type(Type::MULTIFIELD)).expect("Failed to get values argument for set-properties UDF") {
                    ClipsValue::Multifield(mf) => mf
                        .into_iter()
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
                        .collect(),
                    _ => panic!("Expected multifield for values argument in set-properties UDF"),
                };

                let properties: HashMap<String, Value> = props.into_iter().zip(vals).collect();
                trace!("CLIPS UDF 'set-properties' called with object_id='{}' and properties={:?}", object_id, properties);
                match state.set_properties(env, &object_id, &properties) {
                    Ok(_) => {
                        trace!("Successfully set properties {:?} for object '{}'", properties, object_id);
                        let _ = event_tx_set_properties.send(KnowledgeBaseEvent::UpdatedProperties(object_id.clone(), properties.clone()));
                    }
                    Err(e) => {
                        error!("Error setting properties {:?} for object '{}': {}", properties, object_id, e);
                    }
                }

                ClipsValue::Void()
            })
            .expect("Failed to add CLIPS function");

            let state_add_data = Rc::clone(&state);
            let event_tx_add_data = event_tx.clone();
            env.add_udf("add-data", None, 3, 4, vec![Type(Type::SYMBOL), Type(Type::MULTIFIELD), Type(Type::MULTIFIELD), Type(Type::INTEGER)], move |env, ctx| {
                let state = &mut *state_add_data.borrow_mut();
                let object_id = match ctx.get_next_argument(Type(Type::SYMBOL)).expect("Failed to get object ID argument for prompt UDF") {
                    ClipsValue::Symbol(s) => s.to_string(),
                    _ => panic!("Expected symbol for object ID argument in prompt UDF"),
                };
                let args: Vec<String> = match ctx.get_next_argument(Type(Type::MULTIFIELD)).expect("Failed to get args argument for add-data UDF") {
                    ClipsValue::Multifield(mf) => mf
                        .into_iter()
                        .map(|v| match v {
                            ClipsValue::Symbol(s) => s,
                            _ => panic!("Expected symbol, integer, or float in args multifield for add-data UDF"),
                        })
                        .collect(),
                    _ => panic!("Expected multifield for args argument in add-data UDF"),
                };
                let vals: Vec<Value> = match ctx.get_next_argument(Type(Type::MULTIFIELD)).expect("Failed to get values argument for add-data UDF") {
                    ClipsValue::Multifield(mf) => mf
                        .into_iter()
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
                        .collect(),
                    _ => panic!("Expected multifield for values argument in add-data UDF"),
                };
                let date_time = if let Some(arg) = ctx.has_next_argument().then(|| ctx.get_next_argument(Type(Type::INTEGER)).expect("Failed to get date_time argument for add-data UDF")) {
                    if let ClipsValue::Integer(i) = arg {
                        DateTime::<Utc>::from_timestamp(i, 0).expect("Failed to convert date_time argument in add-data UDF")
                    } else {
                        panic!("Expected integer for date_time argument in add-data UDF");
                    }
                } else {
                    Utc::now()
                };

                let values: HashMap<String, Value> = args.into_iter().zip(vals).collect();
                trace!("CLIPS UDF 'add-data' called with object_id='{}', values={:?}, and date_time={}", object_id, values, date_time);
                match state.add_values(env, &object_id, &values, date_time) {
                    Ok(_) => {
                        trace!("Successfully added values {:?} to object '{}' at {}", values, object_id, date_time);
                        let _ = event_tx_add_data.send(KnowledgeBaseEvent::AddedValues(object_id.clone(), values.clone(), date_time));
                    }
                    Err(e) => {
                        error!("Error adding values {:?} to object '{}' at {}: {}", values, object_id, date_time, e);
                    }
                }

                ClipsValue::Void()
            })
            .expect("Failed to add CLIPS function");

            let state_build = Rc::clone(&state);
            while let Some(cmd) = rx.blocking_recv() {
                match cmd {
                    KBCommand::CreateClass(class, resp_tx) => {
                        let result = state_build.borrow_mut().create_class(&mut env, class);
                        if result.is_ok() {
                            env.run(-1);
                        }
                        let _ = resp_tx.send(result);
                    }
                    KBCommand::CreateRule(rule, reply) => {
                        let result = state_build.borrow_mut().create_rule(&mut env, rule);
                        if result.is_ok() {
                            env.run(-1);
                        }
                        let _ = reply.send(result);
                    }
                    KBCommand::CreateObject(object, reply) => {
                        let result = state_build.borrow_mut().create_object(&mut env, object);
                        if result.is_ok() {
                            env.run(-1);
                        }
                        let _ = reply.send(result);
                    }
                    KBCommand::GetStaticProperties(class_names, reply) => {
                        let _ = reply.send(state_build.borrow().get_static_properties(class_names));
                    }
                    KBCommand::GetDynamicProperties(class_names, reply) => {
                        let _ = reply.send(state_build.borrow().get_dynamic_properties(class_names));
                    }
                    KBCommand::AddClass(object_id, class_name, reply) => {
                        let result = state_build.borrow_mut().add_class(&mut env, &object_id, &class_name);
                        if result.is_ok() {
                            env.run(-1);
                        }
                        let _ = reply.send(result);
                    }
                    KBCommand::GetObjectClasses(object_id, reply) => {
                        let state = state_build.borrow();
                        let _ = reply.send(state.objects.get(&object_id).ok_or(KnowledgeBaseError::ObjectNotFound(object_id)).and_then(|object| state.get_object_classes(object)));
                    }
                    KBCommand::SetProperties(object_id, properties, reply) => {
                        let result = state_build.borrow_mut().set_properties(&mut env, &object_id, &properties);
                        if result.is_ok() {
                            env.run(-1);
                        }
                        let _ = reply.send(result);
                    }
                    KBCommand::AddValues(object_id, values, date_time, reply) => {
                        let result = state_build.borrow_mut().add_values(&mut env, &object_id, &values, date_time);
                        if result.is_ok() {
                            env.run(-1);
                        }
                        let _ = reply.send(result);
                    }
                    KBCommand::AddUDF(name, return_type, min_args, max_args, arg_types, func, reply) => {
                        let _ = reply.send(env.add_udf(&name, return_type, min_args, max_args, arg_types, func).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to add UDF {}: {}", name, e))));
                    }
                }
            }
        });

        (CLIPSKnowledgeBase { command_tx: tx }, event_rx)
    }

    pub async fn add_udf(&self, name: &str, return_type: Option<Type>, min_args: u16, max_args: u16, arg_types: Vec<Type>, func: Udf) -> Result<(), KnowledgeBaseError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(KBCommand::AddUDF(name.to_owned(), return_type, min_args, max_args, arg_types, func, resp_tx)).map_err(|_| KnowledgeBaseError::KBError("Failed to send AddUdf command to CLIPS knowledge base actor".to_owned()))?;
        resp_rx.await.map_err(|_| KnowledgeBaseError::KBError("Failed to receive response for AddUdf command from CLIPS knowledge base actor".to_owned()))?
    }
}

#[async_trait]
impl KnowledgeBase for CLIPSKnowledgeBase {
    async fn create_class(&self, class: Class) -> Result<(), KnowledgeBaseError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(KBCommand::CreateClass(class, resp_tx)).map_err(|_| KnowledgeBaseError::KBError("Failed to send CreateClass command to CLIPS knowledge base actor".to_owned()))?;
        resp_rx.await.map_err(|_| KnowledgeBaseError::KBError("Failed to receive response for CreateClass command from CLIPS knowledge base actor".to_owned()))?
    }
    async fn get_static_properties(&self, classe_names: HashSet<String>) -> Result<HashMap<String, HashMap<String, Property>>, KnowledgeBaseError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(KBCommand::GetStaticProperties(classe_names, resp_tx)).map_err(|_| KnowledgeBaseError::KBError("Failed to send GetStaticProperties command to CLIPS knowledge base actor".to_owned()))?;
        resp_rx.await.map_err(|_| KnowledgeBaseError::KBError("Failed to receive response for GetStaticProperties command from CLIPS knowledge base actor".to_owned()))?
    }
    async fn get_dynamic_properties(&self, classe_names: HashSet<String>) -> Result<HashMap<String, HashMap<String, Property>>, KnowledgeBaseError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(KBCommand::GetDynamicProperties(classe_names, resp_tx)).map_err(|_| KnowledgeBaseError::KBError("Failed to send GetDynamicProperties command to CLIPS knowledge base actor".to_owned()))?;
        resp_rx.await.map_err(|_| KnowledgeBaseError::KBError("Failed to receive response for GetDynamicProperties command from CLIPS knowledge base actor".to_owned()))?
    }

    async fn create_rule(&self, rule: Rule) -> Result<(), KnowledgeBaseError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(KBCommand::CreateRule(rule, resp_tx)).map_err(|_| KnowledgeBaseError::KBError("Failed to send CreateRule command to CLIPS knowledge base actor".to_owned()))?;
        resp_rx.await.map_err(|_| KnowledgeBaseError::KBError("Failed to receive response for CreateRule command from CLIPS knowledge base actor".to_owned()))?
    }

    async fn create_object(&self, object: Object) -> Result<(), KnowledgeBaseError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(KBCommand::CreateObject(object, resp_tx)).map_err(|_| KnowledgeBaseError::KBError("Failed to send CreateObject command to CLIPS knowledge base actor".to_owned()))?;
        resp_rx.await.map_err(|_| KnowledgeBaseError::KBError("Failed to receive response for CreateObject command from CLIPS knowledge base actor".to_owned()))?
    }
    async fn add_class(&self, object_id: String, class_name: String) -> Result<(), KnowledgeBaseError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(KBCommand::AddClass(object_id, class_name, resp_tx)).map_err(|_| KnowledgeBaseError::KBError("Failed to send AddClass command to CLIPS knowledge base actor".to_owned()))?;
        resp_rx.await.map_err(|_| KnowledgeBaseError::KBError("Failed to receive response for AddClass command from CLIPS knowledge base actor".to_owned()))?
    }
    async fn get_object_classes(&self, object_id: String) -> Result<HashSet<String>, KnowledgeBaseError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(KBCommand::GetObjectClasses(object_id, resp_tx)).map_err(|_| KnowledgeBaseError::KBError("Failed to send GetObjectClasses command to CLIPS knowledge base actor".to_owned()))?;
        resp_rx.await.map_err(|_| KnowledgeBaseError::KBError("Failed to receive response for GetObjectClasses command from CLIPS knowledge base actor".to_owned()))?
    }
    async fn set_properties(&self, object_id: String, properties: HashMap<String, Value>) -> Result<(), KnowledgeBaseError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(KBCommand::SetProperties(object_id, properties, resp_tx)).map_err(|_| KnowledgeBaseError::KBError("Failed to send SetProperties command to CLIPS knowledge base actor".to_owned()))?;
        resp_rx.await.map_err(|_| KnowledgeBaseError::KBError("Failed to receive response for SetProperties command from CLIPS knowledge base actor".to_owned()))?
    }
    async fn add_values(&self, object_id: String, values: HashMap<String, Value>, date_time: DateTime<Utc>) -> Result<(), KnowledgeBaseError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(KBCommand::AddValues(object_id, values, date_time, resp_tx)).map_err(|_| KnowledgeBaseError::KBError("Failed to send AddValues command to CLIPS knowledge base actor".to_owned()))?;
        resp_rx.await.map_err(|_| KnowledgeBaseError::KBError("Failed to receive response for AddValues command from CLIPS knowledge base actor".to_owned()))?
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
