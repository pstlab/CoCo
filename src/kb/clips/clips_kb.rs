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
    GetClasses(oneshot::Sender<Result<Vec<Class>, KnowledgeBaseError>>),
    GetClass(String, oneshot::Sender<Result<Option<Class>, KnowledgeBaseError>>),
    CreateClass(Class, oneshot::Sender<Result<(), KnowledgeBaseError>>),
    GetRules(oneshot::Sender<Result<Vec<Rule>, KnowledgeBaseError>>),
    GetRule(String, oneshot::Sender<Result<Option<Rule>, KnowledgeBaseError>>),
    CreateRule(Rule, oneshot::Sender<Result<(), KnowledgeBaseError>>),
    GetObjects(oneshot::Sender<Result<Vec<Object>, KnowledgeBaseError>>),
    GetObject(String, oneshot::Sender<Result<Option<Object>, KnowledgeBaseError>>),
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

    fn get_class_hierarchy(&self, classes: &HashSet<String>) -> Result<HashSet<String>, KnowledgeBaseError> {
        let mut queue: VecDeque<String> = classes.iter().cloned().collect();
        let mut visited: HashSet<String> = HashSet::new();

        while let Some(class_name) = queue.pop_front() {
            if visited.contains(&class_name) {
                continue;
            }

            let class = self.classes.get(&class_name).ok_or_else(|| KnowledgeBaseError::ClassNotFound(format!("Class {} not found", class_name)))?;
            if let Some(parents) = &class.parents {
                for parent in parents {
                    if !visited.contains(parent) {
                        queue.push_back(parent.clone());
                    }
                }
            }

            visited.insert(class_name);
        }

        Ok(visited)
    }

    fn get_static_properties(&self, classes: &HashSet<String>) -> Result<HashMap<String, HashMap<String, Property>>, KnowledgeBaseError> {
        let all_classes = self.get_class_hierarchy(classes)?;
        let mut class_properties = HashMap::new();

        for class_name in all_classes {
            let class = self.classes.get(&class_name).ok_or_else(|| KnowledgeBaseError::ClassNotFound(format!("Class {} not found", class_name)))?;
            let mut properties = HashMap::new();
            if let Some(static_props) = &class.static_properties {
                for (name, property) in static_props {
                    properties.entry(name.clone()).or_insert(property.clone());
                }
            }

            if !properties.is_empty() {
                class_properties.insert(class_name, properties);
            }
        }

        Ok(class_properties)
    }

    fn get_dynamic_properties(&self, classes: &HashSet<String>) -> Result<HashMap<String, HashMap<String, Property>>, KnowledgeBaseError> {
        let all_classes = self.get_class_hierarchy(classes)?;
        let mut class_properties = HashMap::new();

        for class_name in all_classes {
            let class = self.classes.get(&class_name).ok_or_else(|| KnowledgeBaseError::ClassNotFound(format!("Class {} not found", class_name)))?;
            let mut properties = HashMap::new();
            if let Some(dynamic_props) = &class.dynamic_properties {
                for (name, property) in dynamic_props {
                    properties.entry(name.clone()).or_insert(property.clone());
                }
            }

            if !properties.is_empty() {
                class_properties.insert(class_name, properties);
            }
        }

        Ok(class_properties)
    }

    fn get_classes(&self) -> Vec<Class> {
        self.classes.values().cloned().collect()
    }

    fn get_class(&self, name: &str) -> Option<Class> {
        self.classes.get(name).cloned()
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

    fn get_rules(&self) -> Vec<Rule> {
        self.rules.values().cloned().collect()
    }

    fn get_rule(&self, name: &str) -> Option<Rule> {
        self.rules.get(name).cloned()
    }

    fn create_rule(&mut self, env: &mut Environment, rule: Rule) -> Result<(), KnowledgeBaseError> {
        if self.rules.contains_key(&rule.name) {
            return Err(KnowledgeBaseError::RuleAlreadyExists(rule.name.clone()));
        }

        env.build(rule.content.as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create rule in CLIPS: {}", e)))?;
        self.rules.insert(rule.name.clone(), rule);

        Ok(())
    }

    fn get_objects(&self) -> Vec<Object> {
        self.objects.values().cloned().collect()
    }

    fn get_object(&self, object_id: String) -> Option<Object> {
        self.objects.get(&object_id).cloned()
    }

    fn create_object(&mut self, env: &mut Environment, object: Object) -> Result<(), KnowledgeBaseError> {
        let object_id = object.id.clone().ok_or(KnowledgeBaseError::ObjectIDRequired)?;
        if self.objects.contains_key(&object_id) {
            return Err(KnowledgeBaseError::ObjectAlreadyExists(object_id));
        }

        let classes = self.get_class_hierarchy(&object.classes)?;
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

        for (class_name, props) in self.get_static_properties(&object.classes)? {
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
                    let fb = set_prop(env, fb, &prop, def, None).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set default value for property {} of object {}: {:#?}", name, object_id, e)))?;
                    let fact = env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for default value of property {} of object {}: {}", name, object_id, e)))?;
                    self.values.entry(class_name.clone()).or_default().entry(object_id.clone()).or_default().insert(name.clone(), fact);
                }
            }
        }

        for (class_name, props) in self.get_dynamic_properties(&object.classes)? {
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
                    let fb = set_prop(env, fb, &prop, def, Some(Utc::now())).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set default value for dynamic property {} of object {}: {:#?}", name, object_id, e)))?;
                    let fact = env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for default value of dynamic property {} of object {}: {}", name, object_id, e)))?;
                    self.values.entry(class_name.clone()).or_default().entry(object_id.clone()).or_default().insert(name.clone(), fact);
                }
            }
        }

        self.objects.insert(object_id, object);

        Ok(())
    }

    fn add_class(&mut self, env: &mut Environment, object_id: &str, class_name: &str) -> Result<(), KnowledgeBaseError> {
        let single_class = HashSet::from([class_name.to_owned()]);
        let static_props = self.get_static_properties(&single_class)?;
        let dynamic_props = self.get_dynamic_properties(&single_class)?;
        let mut object_classes = self.objects.get(object_id).ok_or_else(|| KnowledgeBaseError::ObjectNotFound(object_id.to_owned()))?.classes.clone();
        object_classes.insert(class_name.to_owned());
        let classes = self.get_class_hierarchy(&object_classes)?;
        let object_id_owned = object_id.to_owned();

        let object = self.objects.get_mut(object_id).ok_or_else(|| KnowledgeBaseError::ObjectNotFound(object_id.to_owned()))?;
        object.classes.insert(class_name.to_owned());

        for class_name in classes {
            if !self.classes.contains_key(&class_name) {
                return Err(KnowledgeBaseError::ClassNotFound(format!("Class {} not found for object {}", class_name, object_id)));
            }
            let fb = env.fact_builder(&class_name).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact builder for class {}: {}", class_name, e)))?;
            let fb = fb.put_symbol("id", object_id).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set id slot for object {}: {}", object_id, e)))?;
            let fact = env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for object {}: {}", object_id, e)))?;
            self.instances.entry(class_name).or_default().insert(object_id_owned.clone(), fact);
        }

        for (class_name, props) in static_props {
            let class_values = self.values.entry(class_name.clone()).or_default().entry(object_id_owned.clone()).or_default();
            for (name, prop) in props {
                let fb = env.fact_builder(&format!("{}_{}", class_name, name)).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact builder for property {} of object {}: {}", name, object_id, e)))?;
                let fb = fb.put_symbol("id", object_id).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set id slot for property {} of object {}: {}", name, object_id, e)))?;
                if let Some(v) = object.properties.as_ref().and_then(|props| props.get(&name)) {
                    let fb: FactBuilder = set_prop(env, fb, &prop, v.clone(), None).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set property {} for object {}: {:#?}", name, object_id, e)))?;
                    let fact = env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for property {} of object {}: {}", name, object_id, e)))?;
                    class_values.insert(name.clone(), fact);
                } else {
                    let def = get_default(&prop);
                    let fb = set_prop(env, fb, &prop, def, None).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set default value for property {} of object {}: {:#?}", name, object_id, e)))?;
                    let fact = env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for default value of property {} of object {}: {}", name, object_id, e)))?;
                    class_values.insert(name.clone(), fact);
                }
            }
        }

        for (class_name, props) in dynamic_props {
            let class_values = self.values.entry(class_name.clone()).or_default().entry(object_id_owned.clone()).or_default();
            for (name, prop) in props {
                let fb = env.fact_builder(&format!("{}_{}", class_name, name)).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact builder for dynamic property {} of object {}: {}", name, object_id, e)))?;
                if let Some(v) = object.values.as_ref().and_then(|vals| vals.get(&name)) {
                    let fb = set_prop(env, fb, &prop, v.value.clone(), Some(v.timestamp)).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set dynamic property {} for object {}: {:#?}", name, object_id, e)))?;
                    let fact = env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for dynamic property {} of object {}: {}", name, object_id, e)))?;
                    class_values.insert(name.clone(), fact);
                } else {
                    let def = get_default(&prop);
                    let fb = set_prop(env, fb, &prop, def, Some(Utc::now()))?;
                    let fact = env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for default value of dynamic property {} of object {}: {}", name, object_id, e)))?;
                    class_values.insert(name.clone(), fact);
                }
            }
        }

        Ok(())
    }

    fn set_properties(&mut self, env: &mut Environment, object_id: &str, properties: &HashMap<String, Value>) -> Result<(), KnowledgeBaseError> {
        let static_props = self.get_static_properties(&self.objects.get(object_id).ok_or_else(|| KnowledgeBaseError::ObjectNotFound(object_id.to_owned()))?.classes)?;
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
        let dynamic_props = self.get_dynamic_properties(&self.objects.get(object_id).ok_or_else(|| KnowledgeBaseError::ObjectNotFound(object_id.to_owned()))?.classes)?;
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
                let object_id = match ctx.get_next_argument(Type(Type::SYMBOL)) {
                    Some(ClipsValue::Symbol(s)) => s,
                    _ => {
                        error!("Expected symbol for object ID argument in add-class UDF");
                        return ClipsValue::Void();
                    }
                };
                let class_name = match ctx.get_next_argument(Type(Type::SYMBOL)) {
                    Some(ClipsValue::Symbol(s)) => s,
                    _ => {
                        error!("Expected symbol for class name argument in add-class UDF");
                        return ClipsValue::Void();
                    }
                };

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
                let object_id = match ctx.get_next_argument(Type(Type::SYMBOL)) {
                    Some(ClipsValue::Symbol(s)) => s.to_string(),
                    _ => {
                        error!("Expected symbol for object ID argument in set-properties UDF");
                        return ClipsValue::Void();
                    }
                };
                let props: Vec<String> = match ctx.get_next_argument(Type(Type::MULTIFIELD)) {
                    Some(ClipsValue::Multifield(mf)) => {
                        let mut out = Vec::with_capacity(mf.len());
                        for v in mf {
                            match v {
                                ClipsValue::Symbol(s) => out.push(s),
                                _ => {
                                    error!("Expected symbol in properties multifield for set-properties UDF");
                                    return ClipsValue::Void();
                                }
                            }
                        }
                        out
                    }
                    _ => {
                        error!("Expected multifield for properties argument in set-properties UDF");
                        return ClipsValue::Void();
                    }
                };
                let vals: Vec<Value> = match ctx.get_next_argument(Type(Type::MULTIFIELD)) {
                    Some(ClipsValue::Multifield(mf)) => {
                        let mut out = Vec::with_capacity(mf.len());
                        for v in mf {
                            let value = match v {
                                ClipsValue::Integer(i) => Value::Int(i),
                                ClipsValue::Float(f) => Value::Float(f),
                                ClipsValue::Symbol(s) => match s.as_str() {
                                    "TRUE" => Value::Bool(true),
                                    "FALSE" => Value::Bool(false),
                                    "nil" => Value::Null,
                                    other => Value::Symbol(other.to_owned()),
                                },
                                ClipsValue::String(s) => Value::String(s),
                                _ => {
                                    error!("Expected symbol, integer, float, or string in values multifield for set-properties UDF");
                                    return ClipsValue::Void();
                                }
                            };
                            out.push(value);
                        }
                        out
                    }
                    _ => {
                        error!("Expected multifield for values argument in set-properties UDF");
                        return ClipsValue::Void();
                    }
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
                let object_id = match ctx.get_next_argument(Type(Type::SYMBOL)) {
                    Some(ClipsValue::Symbol(s)) => s.to_string(),
                    _ => {
                        error!("Expected symbol for object ID argument in add-data UDF");
                        return ClipsValue::Void();
                    }
                };
                let args: Vec<String> = match ctx.get_next_argument(Type(Type::MULTIFIELD)) {
                    Some(ClipsValue::Multifield(mf)) => {
                        let mut out = Vec::with_capacity(mf.len());
                        for v in mf {
                            match v {
                                ClipsValue::Symbol(s) => out.push(s),
                                _ => {
                                    error!("Expected symbol in args multifield for add-data UDF");
                                    return ClipsValue::Void();
                                }
                            }
                        }
                        out
                    }
                    _ => {
                        error!("Expected multifield for args argument in add-data UDF");
                        return ClipsValue::Void();
                    }
                };
                let vals: Vec<Value> = match ctx.get_next_argument(Type(Type::MULTIFIELD)) {
                    Some(ClipsValue::Multifield(mf)) => {
                        let mut out = Vec::with_capacity(mf.len());
                        for v in mf {
                            let value = match v {
                                ClipsValue::Integer(i) => Value::Int(i),
                                ClipsValue::Float(f) => Value::Float(f),
                                ClipsValue::Symbol(s) => match s.as_str() {
                                    "TRUE" => Value::Bool(true),
                                    "FALSE" => Value::Bool(false),
                                    "nil" => Value::Null,
                                    other => Value::Symbol(other.to_owned()),
                                },
                                ClipsValue::String(s) => Value::String(s),
                                _ => {
                                    error!("Expected symbol, integer, float, or string in values multifield for add-data UDF");
                                    return ClipsValue::Void();
                                }
                            };
                            out.push(value);
                        }
                        out
                    }
                    _ => {
                        error!("Expected multifield for values argument in add-data UDF");
                        return ClipsValue::Void();
                    }
                };
                let date_time = if ctx.has_next_argument() {
                    match ctx.get_next_argument(Type(Type::INTEGER)) {
                        Some(ClipsValue::Integer(ts)) => match DateTime::<Utc>::from_timestamp(ts, 0) {
                            Some(dt) => dt,
                            None => {
                                error!("Invalid timestamp for date_time argument in add-data UDF: {}", ts);
                                return ClipsValue::Void();
                            }
                        },
                        _ => {
                            error!("Expected integer for date_time argument in add-data UDF");
                            return ClipsValue::Void();
                        }
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
                    KBCommand::GetClasses(reply) => {
                        let _ = reply.send(Ok(state_build.borrow().get_classes()));
                    }
                    KBCommand::GetClass(name, reply) => {
                        let _ = reply.send(Ok(state_build.borrow().get_class(&name)));
                    }
                    KBCommand::CreateClass(class, resp_tx) => {
                        let result = state_build.borrow_mut().create_class(&mut env, class);
                        if result.is_ok() {
                            env.run(-1);
                        }
                        let _ = resp_tx.send(result);
                    }
                    KBCommand::GetRules(reply) => {
                        let _ = reply.send(Ok(state_build.borrow().get_rules()));
                    }
                    KBCommand::GetRule(name, reply) => {
                        let _ = reply.send(Ok(state_build.borrow().get_rule(&name)));
                    }
                    KBCommand::CreateRule(rule, reply) => {
                        let result = state_build.borrow_mut().create_rule(&mut env, rule);
                        if result.is_ok() {
                            env.run(-1);
                        }
                        let _ = reply.send(result);
                    }
                    KBCommand::GetObjects(reply) => {
                        let _ = reply.send(Ok(state_build.borrow().get_objects()));
                    }
                    KBCommand::GetObject(object_id, reply) => {
                        let _ = reply.send(Ok(state_build.borrow().get_object(object_id)));
                    }
                    KBCommand::CreateObject(object, reply) => {
                        let result = state_build.borrow_mut().create_object(&mut env, object);
                        if result.is_ok() {
                            env.run(-1);
                        }
                        let _ = reply.send(result);
                    }
                    KBCommand::GetStaticProperties(class_names, reply) => {
                        let _ = reply.send(state_build.borrow().get_static_properties(&class_names));
                    }
                    KBCommand::GetDynamicProperties(class_names, reply) => {
                        let _ = reply.send(state_build.borrow().get_dynamic_properties(&class_names));
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
                        let _ = reply.send(state.objects.get(&object_id).ok_or(KnowledgeBaseError::ObjectNotFound(object_id)).and_then(|object| state.get_class_hierarchy(&object.classes)));
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
    async fn get_classes(&self) -> Result<Vec<Class>, KnowledgeBaseError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(KBCommand::GetClasses(resp_tx)).map_err(|_| KnowledgeBaseError::KBError("Failed to send GetClasses command to CLIPS knowledge base actor".to_owned()))?;
        resp_rx.await.map_err(|_| KnowledgeBaseError::KBError("Failed to receive response for GetClasses command from CLIPS knowledge base actor".to_owned()))?
    }
    async fn get_class(&self, name: &str) -> Result<Option<Class>, KnowledgeBaseError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(KBCommand::GetClass(name.to_owned(), resp_tx)).map_err(|_| KnowledgeBaseError::KBError("Failed to send GetClass command to CLIPS knowledge base actor".to_owned()))?;
        resp_rx.await.map_err(|_| KnowledgeBaseError::KBError("Failed to receive response for GetClass command from CLIPS knowledge base actor".to_owned()))?
    }
    async fn create_class(&self, class: Class) -> Result<(), KnowledgeBaseError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(KBCommand::CreateClass(class, resp_tx)).map_err(|_| KnowledgeBaseError::KBError("Failed to send CreateClass command to CLIPS knowledge base actor".to_owned()))?;
        resp_rx.await.map_err(|_| KnowledgeBaseError::KBError("Failed to receive response for CreateClass command from CLIPS knowledge base actor".to_owned()))?
    }
    async fn get_static_properties(&self, class_names: HashSet<String>) -> Result<HashMap<String, HashMap<String, Property>>, KnowledgeBaseError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(KBCommand::GetStaticProperties(class_names, resp_tx)).map_err(|_| KnowledgeBaseError::KBError("Failed to send GetStaticProperties command to CLIPS knowledge base actor".to_owned()))?;
        resp_rx.await.map_err(|_| KnowledgeBaseError::KBError("Failed to receive response for GetStaticProperties command from CLIPS knowledge base actor".to_owned()))?
    }
    async fn get_dynamic_properties(&self, class_names: HashSet<String>) -> Result<HashMap<String, HashMap<String, Property>>, KnowledgeBaseError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(KBCommand::GetDynamicProperties(class_names, resp_tx)).map_err(|_| KnowledgeBaseError::KBError("Failed to send GetDynamicProperties command to CLIPS knowledge base actor".to_owned()))?;
        resp_rx.await.map_err(|_| KnowledgeBaseError::KBError("Failed to receive response for GetDynamicProperties command from CLIPS knowledge base actor".to_owned()))?
    }

    async fn get_rules(&self) -> Result<Vec<Rule>, KnowledgeBaseError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(KBCommand::GetRules(resp_tx)).map_err(|_| KnowledgeBaseError::KBError("Failed to send GetRules command to CLIPS knowledge base actor".to_owned()))?;
        resp_rx.await.map_err(|_| KnowledgeBaseError::KBError("Failed to receive response for GetRules command from CLIPS knowledge base actor".to_owned()))?
    }
    async fn get_rule(&self, name: &str) -> Result<Option<Rule>, KnowledgeBaseError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(KBCommand::GetRule(name.to_owned(), resp_tx)).map_err(|_| KnowledgeBaseError::KBError("Failed to send GetRule command to CLIPS knowledge base actor".to_owned()))?;
        resp_rx.await.map_err(|_| KnowledgeBaseError::KBError("Failed to receive response for GetRule command from CLIPS knowledge base actor".to_owned()))?
    }
    async fn create_rule(&self, rule: Rule) -> Result<(), KnowledgeBaseError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(KBCommand::CreateRule(rule, resp_tx)).map_err(|_| KnowledgeBaseError::KBError("Failed to send CreateRule command to CLIPS knowledge base actor".to_owned()))?;
        resp_rx.await.map_err(|_| KnowledgeBaseError::KBError("Failed to receive response for CreateRule command from CLIPS knowledge base actor".to_owned()))?
    }

    async fn get_objects(&self) -> Result<Vec<Object>, KnowledgeBaseError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(KBCommand::GetObjects(resp_tx)).map_err(|_| KnowledgeBaseError::KBError("Failed to send GetObjects command to CLIPS knowledge base actor".to_owned()))?;
        resp_rx.await.map_err(|_| KnowledgeBaseError::KBError("Failed to receive response for GetObjects command from CLIPS knowledge base actor".to_owned()))?
    }
    async fn get_object(&self, object_id: String) -> Result<Option<Object>, KnowledgeBaseError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx.send(KBCommand::GetObject(object_id, resp_tx)).map_err(|_| KnowledgeBaseError::KBError("Failed to send GetObject command to CLIPS knowledge base actor".to_owned()))?;
        resp_rx.await.map_err(|_| KnowledgeBaseError::KBError("Failed to receive response for GetObject command from CLIPS knowledge base actor".to_owned()))?
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
        Property::Bool { default, .. } => {
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
        Property::Int { default, min, max, .. } => {
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
        Property::Float { default, min, max, .. } => {
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
        Property::String { default, .. } => {
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
        Property::Symbol { default, allowed_values, .. } => {
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
        Property::BoolArray { default, .. } => {
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
        Property::IntArray { default, min, max, .. } => {
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
        Property::FloatArray { default, min, max, .. } => {
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
        Property::StringArray { default, .. } => {
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
        Property::SymbolArray { default, allowed_values, .. } => {
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
        Property::BoolArray { default, .. } => default.clone().map(Value::BoolArray).unwrap_or(Value::Null),
        Property::IntArray { default, .. } => default.clone().map(Value::IntArray).unwrap_or(Value::Null),
        Property::FloatArray { default, .. } => default.clone().map(Value::FloatArray).unwrap_or(Value::Null),
        Property::StringArray { default, .. } => default.clone().map(Value::StringArray).unwrap_or(Value::Null),
        Property::SymbolArray { default, .. } => default.clone().map(Value::StringArray).unwrap_or(Value::Null),
        Property::ObjectArray { default, .. } => default.clone().map(Value::StringArray).unwrap_or(Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_class() -> Class {
        Class {
            name: "device".to_owned(),
            parents: None,
            static_properties: None,
            dynamic_properties: None,
        }
    }

    #[test]
    fn prop_deftemplate_covers_scalar_variants() {
        let class = test_class();

        let bool_def = prop_deftemplate(&class, "enabled", &Property::Bool { default: Some(true), description: None }, true);
        assert!(bool_def.contains("(slot value (type SYMBOL) (allowed-symbols TRUE FALSE nil)"));
        assert!(bool_def.contains("(default TRUE)"));
        assert!(!bool_def.contains("(slot time (type INTEGER))"));

        let int_def = prop_deftemplate(&class, "level", &Property::Int { default: Some(5), min: Some(0), max: Some(10), description: None }, false);
        assert!(int_def.contains("(default 5)"));
        assert!(int_def.contains("(range 0 10)"));
        assert!(int_def.contains("(slot time (type INTEGER))"));

        let float_def = prop_deftemplate(&class, "temperature", &Property::Float { default: Some(42.0), min: Some(1.0), max: Some(99.0), description: None }, true);
        assert!(float_def.contains("(default 42.0)"));
        assert!(float_def.contains("(range 1.0 99.0)"));

        let string_def = prop_deftemplate(&class, "name", &Property::String { default: Some("sensor".to_owned()), description: None }, false);
        assert!(string_def.contains("(default \"sensor\")"));
        assert!(string_def.contains("(slot time (type INTEGER))"));

        let mut allowed_symbols = HashSet::new();
        allowed_symbols.insert("ok".to_owned());
        allowed_symbols.insert("error".to_owned());
        let symbol_def = prop_deftemplate(&class, "status", &Property::Symbol { default: Some("ok".to_owned()), allowed_values: Some(allowed_symbols), description: None }, true);
        assert!(symbol_def.contains("(allowed-symbols nil"));
        assert!(symbol_def.contains("(default ok)"));

        let object_def = prop_deftemplate(&class, "owner", &Property::Object { default: Some("obj_1".to_owned()), classes: vec!["user".to_owned()], description: None }, false);
        assert!(object_def.contains("(default obj_1)"));
        assert!(object_def.contains("(slot time (type INTEGER))"));
    }

    #[test]
    fn prop_deftemplate_covers_array_variants() {
        let class = test_class();

        let bool_array_def = prop_deftemplate(&class, "flags", &Property::BoolArray { default: Some(vec![true, false]), description: None }, true);
        assert!(bool_array_def.contains("(multislot value (type SYMBOL) (allowed-symbols TRUE FALSE nil)"));
        assert!(bool_array_def.contains("(default TRUE FALSE)"));

        let int_array_def = prop_deftemplate(&class, "samples", &Property::IntArray { default: Some(vec![1, 2, 3]), min: Some(0), max: Some(100), description: None }, false);
        assert!(int_array_def.contains("(default 1 2 3)"));
        assert!(int_array_def.contains("(range 0 100)"));
        assert!(int_array_def.contains("(slot time (type INTEGER))"));

        let float_array_def = prop_deftemplate(&class, "weights", &Property::FloatArray { default: Some(vec![1.0, 2.5]), min: Some(0.0), max: Some(10.0), description: None }, true);
        assert!(float_array_def.contains("(default 1.0 2.5)"));
        assert!(float_array_def.contains("(range 0.0 10.0)"));

        let string_array_def = prop_deftemplate(&class, "labels", &Property::StringArray { default: Some(vec!["a".to_owned(), "b".to_owned()]), description: None }, false);
        assert!(string_array_def.contains("(default \"a\" \"b\")"));
        assert!(string_array_def.contains("(slot time (type INTEGER))"));

        let mut allowed_symbols = HashSet::new();
        allowed_symbols.insert("x".to_owned());
        allowed_symbols.insert("y".to_owned());
        let symbol_array_def = prop_deftemplate(
            &class,
            "states",
            &Property::SymbolArray {
                default: Some(vec!["x".to_owned(), "y".to_owned()]),
                allowed_values: Some(allowed_symbols),
                description: None,
            },
            true,
        );
        assert!(symbol_array_def.contains("(allowed-symbols nil"));
        assert!(symbol_array_def.contains("(default x y)"));

        let object_array_def = prop_deftemplate(
            &class,
            "related",
            &Property::ObjectArray {
                default: Some(vec!["obj_a".to_owned(), "obj_b".to_owned()]),
                classes: vec!["device".to_owned()],
                description: None,
            },
            false,
        );
        assert!(object_array_def.contains("(default obj_a obj_b)"));
        assert!(object_array_def.contains("(slot time (type INTEGER))"));
    }

    #[test]
    fn get_default_covers_all_none_defaults() {
        let properties = vec![
            Property::Bool { default: None, description: None },
            Property::Int { default: None, min: None, max: None, description: None },
            Property::Float { default: None, min: None, max: None, description: None },
            Property::String { default: None, description: None },
            Property::Symbol { default: None, allowed_values: None, description: None },
            Property::Object { default: None, classes: vec!["device".to_owned()], description: None },
            Property::BoolArray { default: None, description: None },
            Property::IntArray { default: None, min: None, max: None, description: None },
            Property::FloatArray { default: None, min: None, max: None, description: None },
            Property::StringArray { default: None, description: None },
            Property::SymbolArray { default: None, allowed_values: None, description: None },
            Property::ObjectArray { default: None, classes: vec!["device".to_owned()], description: None },
        ];

        for property in properties {
            assert_eq!(get_default(&property), Value::Null);
        }
    }

    #[test]
    fn get_default_covers_all_some_defaults() {
        assert_eq!(get_default(&Property::Bool { default: Some(true), description: None }), Value::Bool(true));
        assert_eq!(get_default(&Property::Int { default: Some(7), min: None, max: None, description: None }), Value::Int(7));
        assert_eq!(get_default(&Property::Float { default: Some(3.5), min: None, max: None, description: None }), Value::Float(3.5));
        assert_eq!(get_default(&Property::String { default: Some("abc".to_owned()), description: None }), Value::String("abc".to_owned()));
        assert_eq!(get_default(&Property::Symbol { default: Some("ok".to_owned()), allowed_values: None, description: None }), Value::Symbol("ok".to_owned()));
        assert_eq!(get_default(&Property::Object { default: Some("obj_1".to_owned()), classes: vec!["device".to_owned()], description: None }), Value::Object("obj_1".to_owned()));
        assert_eq!(get_default(&Property::BoolArray { default: Some(vec![true, false]), description: None }), Value::BoolArray(vec![true, false]));
        assert_eq!(get_default(&Property::IntArray { default: Some(vec![1, 2]), min: None, max: None, description: None }), Value::IntArray(vec![1, 2]));
        assert_eq!(get_default(&Property::FloatArray { default: Some(vec![1.0, 2.5]), min: None, max: None, description: None }), Value::FloatArray(vec![1.0, 2.5]));
        assert_eq!(get_default(&Property::StringArray { default: Some(vec!["a".to_owned(), "b".to_owned()]), description: None }), Value::StringArray(vec!["a".to_owned(), "b".to_owned()]));

        // get_default currently maps SymbolArray/ObjectArray defaults to Value::StringArray.
        assert_eq!(
            get_default(&Property::SymbolArray {
                default: Some(vec!["x".to_owned(), "y".to_owned()]),
                allowed_values: None,
                description: None,
            }),
            Value::StringArray(vec!["x".to_owned(), "y".to_owned()])
        );
        assert_eq!(
            get_default(&Property::ObjectArray {
                default: Some(vec!["obj_a".to_owned(), "obj_b".to_owned()]),
                classes: vec!["device".to_owned()],
                description: None,
            }),
            Value::StringArray(vec!["obj_a".to_owned(), "obj_b".to_owned()])
        );
    }
}
