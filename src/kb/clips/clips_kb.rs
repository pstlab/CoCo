use crate::{
    kb::{KnowledgeBase, KnowledgeBaseError, KnowledgeBaseEvent},
    model::{Class, Object, Property, Rule, TimedValue, Value},
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clips::{ClipsValue, Environment, Fact, FactBuilder, FactModifier, Type, UDFContext};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, trace};

type Udf = Box<dyn FnMut(&mut Environment, &mut UDFContext) -> ClipsValue + Send>;

enum KBCommand {
    CreateClass(Class, oneshot::Sender<Result<(), KnowledgeBaseError>>),
    CreateRule(Rule, oneshot::Sender<Result<(), KnowledgeBaseError>>),
    CreateObject(Object, oneshot::Sender<Result<(), KnowledgeBaseError>>),
    GetStaticProperties(HashSet<String>, oneshot::Sender<Result<HashMap<String, HashMap<String, Property>>, KnowledgeBaseError>>),
    GetDynamicProperties(HashSet<String>, oneshot::Sender<Result<HashMap<String, HashMap<String, Property>>, KnowledgeBaseError>>),
    AddClass(String, String, oneshot::Sender<Result<(), KnowledgeBaseError>>),
    GetObjectClasses(String, oneshot::Sender<Result<HashSet<String>, KnowledgeBaseError>>),
    SetProperties(String, HashMap<String, Value>, oneshot::Sender<Result<(), KnowledgeBaseError>>),
    AddValues(String, HashMap<String, Value>, DateTime<Utc>, oneshot::Sender<Result<(), KnowledgeBaseError>>),
    Build(String, oneshot::Sender<Result<(), KnowledgeBaseError>>),
    AddUDF(String, Option<Type>, u16, u16, Vec<Type>, Udf, oneshot::Sender<Result<(), KnowledgeBaseError>>),
    AssertFact(String, HashMap<String, Value>, oneshot::Sender<Result<u64, KnowledgeBaseError>>),
    ModifyFact(u64, HashMap<String, Value>, oneshot::Sender<Result<(), KnowledgeBaseError>>),
}

#[derive(Clone)]
pub struct CLIPSKnowledgeBase {
    tx: mpsc::Sender<KBCommand>,
    event_rx: Arc<Mutex<Option<mpsc::Receiver<KnowledgeBaseEvent>>>>,
}

struct ActorState {
    classes: HashMap<String, Class>,
    objects: HashMap<String, Object>,
    rules: HashMap<String, Rule>,

    instances: HashMap<String, HashMap<String, Fact>>,               // class name -> object id -> fact
    values: HashMap<String, HashMap<String, HashMap<String, Fact>>>, // class name -> object id -> property name -> fact
    external_facts: HashMap<u64, Fact>,
    next_fact_id: u64,
    env: Environment,
}

impl ActorState {
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
}

impl Default for CLIPSKnowledgeBase {
    fn default() -> Self {
        Self::new()
    }
}

impl CLIPSKnowledgeBase {
    pub fn new() -> Self {
        let (tx, mut rx) = mpsc::channel(100);
        let (event_tx, event_rx) = mpsc::channel(100);

        info!("Starting CLIPS knowledge base");
        tokio::task::spawn_blocking(move || {
            let env = Environment::new().expect("Failed to create CLIPS environment");
            let mut kb = ActorState {
                classes: HashMap::new(),
                objects: HashMap::new(),
                rules: HashMap::new(),
                instances: HashMap::new(),
                values: HashMap::new(),
                external_facts: HashMap::new(),
                next_fact_id: 0,
                env,
            };

            let add_data_event_tx = event_tx.clone();
            kb.env
                .add_udf("add-data", None, 3, 4, vec![Type(Type::SYMBOL), Type(Type::MULTIFIELD), Type(Type::MULTIFIELD), Type(Type::INTEGER)], move |_env, ctx| {
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

                    add_data_event_tx.blocking_send(KnowledgeBaseEvent::AddedValues(object_id.clone(), args.into_iter().zip(vals).collect(), date_time)).expect("Failed to send AddedValues event from add-data UDF");

                    ClipsValue::Void()
                })
                .expect("Failed to add CLIPS function");

            while let Some(cmd) = rx.blocking_recv() {
                match cmd {
                    KBCommand::CreateClass(class, reply) => {
                        trace!("Creating class: {}", class.name);

                        let result = (|| -> Result<(), KnowledgeBaseError> {
                            if kb.classes.contains_key(&class.name) {
                                return Err(KnowledgeBaseError::ClassAlreadyExists(class.name.clone()));
                            }

                            kb.env.build(format!("(deftemplate {} (slot id (type SYMBOL)))", class.name).as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create class in CLIPS: {}", e)))?;
                            if let Some(static_props) = &class.static_properties {
                                for (name, prop) in static_props {
                                    kb.env.build(prop_deftemplate(&class, name, prop, true).as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create static property {} for class {} in CLIPS: {}", name, class.name, e)))?;
                                }
                            }
                            if let Some(dynamic_props) = &class.dynamic_properties {
                                for (name, prop) in dynamic_props {
                                    kb.env.build(prop_deftemplate(&class, name, prop, false).as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create dynamic property {} for class {} in CLIPS: {}", name, class.name, e)))?;
                                }
                            }
                            kb.classes.insert(class.name.clone(), class);

                            Ok(())
                        })();

                        let _ = reply.send(result);
                    }
                    KBCommand::CreateRule(rule, reply) => {
                        trace!("Creating rule: {}", rule.name);
                        let result = (|| -> Result<(), KnowledgeBaseError> {
                            if kb.rules.contains_key(&rule.name) {
                                return Err(KnowledgeBaseError::RuleAlreadyExists(rule.name.clone()));
                            }

                            kb.env.build(rule.content.as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create rule in CLIPS: {}", e)))?;
                            kb.rules.insert(rule.name.clone(), rule);

                            Ok(())
                        })();

                        let _ = reply.send(result);
                    }
                    KBCommand::CreateObject(object, reply) => {
                        let Some(object_id) = object.id.clone() else {
                            let _ = reply.send(Err(KnowledgeBaseError::CreationError("Object ID is required".to_string())));
                            continue;
                        };
                        if kb.objects.contains_key(&object_id) {
                            let _ = reply.send(Err(KnowledgeBaseError::ObjectAlreadyExists(object_id.clone())));
                            continue;
                        }
                        trace!("Creating object: {}", object_id);

                        let result = (|| -> Result<(), KnowledgeBaseError> {
                            match kb.get_object_classes(&object) {
                                Ok(classes) => {
                                    if classes.is_empty() {
                                        return Err(KnowledgeBaseError::CreationError(format!("Object {} must belong to at least one class", object_id)));
                                    }
                                    for class_name in classes {
                                        if !kb.classes.contains_key(&class_name) {
                                            return Err(KnowledgeBaseError::ClassNotFound(format!("Class {} not found for object {}", class_name, object_id)));
                                        }
                                        let fb = kb.env.fact_builder(&class_name).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact builder for class {}: {}", class_name, e)))?;
                                        let fb = fb.put_symbol("id", object_id.as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set id slot for object {}: {}", object_id, e)))?;
                                        let fact = kb.env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for object {}: {}", object_id, e)))?;
                                        kb.instances.entry(class_name).or_default().insert(object_id.clone(), fact);
                                    }
                                }
                                Err(e) => {
                                    error!("Error getting classes for object {}: {}", object_id, e);
                                    return Err(e);
                                }
                            }

                            match kb.get_static_properties(object.classes.iter().cloned().collect()) {
                                Ok(class_props) => {
                                    for (class_name, props) in class_props {
                                        let class = kb.classes.get(&class_name).ok_or_else(|| KnowledgeBaseError::ClassNotFound(format!("Class {} not found for object {}", class_name, object_id)))?;
                                        for (name, prop) in props {
                                            let fb = kb.env.fact_builder(&format!("{}_{}", class.name, name)).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact builder for property {} of object {}: {}", name, object_id, e)))?;
                                            let fb = fb.put_symbol("id", object_id.as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set id slot for property {} of object {}: {}", name, object_id, e)))?;
                                            if let Some(v) = object.properties.as_ref().and_then(|props| props.get(&name)) {
                                                let fb: FactBuilder = set_prop(&kb.env, fb, &prop, v.clone(), None).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set property {} for object {}: {:#?}", name, object_id, e)))?;
                                                let fact = kb.env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for property {} of object {}: {}", name, object_id, e)))?;
                                                kb.values.entry(class.name.clone()).or_default().entry(object_id.clone()).or_default().insert(name.clone(), fact);
                                            } else {
                                                let def = get_default(&prop);
                                                let fb = set_prop(&kb.env, fb, &prop, def.clone(), None).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set default value for property {} of object {}: {:#?}", name, object_id, e)))?;
                                                let fact = kb.env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for default value of property {} of object {}: {}", name, object_id, e)))?;
                                                kb.values.entry(class.name.clone()).or_default().entry(object_id.clone()).or_default().insert(name.clone(), fact);
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("Error getting static properties for object {}: {}", object_id, e);
                                    return Err(e);
                                }
                            }

                            match kb.get_dynamic_properties(object.classes.iter().cloned().collect()) {
                                Ok(class_props) => {
                                    for (class_name, props) in class_props {
                                        let class = kb.classes.get(&class_name).ok_or_else(|| KnowledgeBaseError::ClassNotFound(format!("Class {} not found for object {}", class_name, object_id)))?;
                                        for (name, prop) in props {
                                            let fb = kb.env.fact_builder(&format!("{}_{}", class.name, name)).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact builder for dynamic property {} of object {}: {}", name, object_id, e)))?;
                                            let fb = fb.put_symbol("id", object_id.as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set id slot for dynamic property {} of object {}: {}", name, object_id, e)))?;
                                            if let Some(v) = object.values.as_ref().and_then(|vals| vals.get(&name)) {
                                                let fb = set_prop(&kb.env, fb, &prop, v.value.clone(), Some(v.timestamp)).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set dynamic property {} for object {}: {:#?}", name, object_id, e)))?;
                                                let fact = kb.env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for dynamic property {} of object {}: {}", name, object_id, e)))?;
                                                kb.values.entry(class.name.clone()).or_default().entry(object_id.clone()).or_default().insert(name.clone(), fact);
                                            } else {
                                                let def = get_default(&prop);
                                                let fb = kb.env.fact_builder(&format!("{}_{}", class.name, name)).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact builder for dynamic property {} of object {}: {}", name, object_id, e)))?;
                                                let fb = set_prop(&kb.env, fb, &prop, def.clone(), Some(Utc::now())).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set default value for dynamic property {} of object {}: {:#?}", name, object_id, e)))?;
                                                let fact = kb.env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for default value of dynamic property {} of object {}: {}", name, object_id, e)))?;
                                                kb.values.entry(class.name.clone()).or_default().entry(object_id.clone()).or_default().insert(name.clone(), fact);
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("Error getting dynamic properties for object {}: {}", object_id, e);
                                    return Err(e);
                                }
                            }

                            kb.objects.insert(object_id, object);
                            Ok(())
                        })();

                        let _ = reply.send(result);
                    }
                    KBCommand::GetStaticProperties(classes, reply) => {
                        trace!("Getting static properties for classes: {:?}", classes);
                        let result = (|| -> Result<HashMap<String, HashMap<String, Property>>, KnowledgeBaseError> { kb.get_static_properties(classes) })();

                        let _ = reply.send(result);
                    }
                    KBCommand::GetDynamicProperties(classes, reply) => {
                        trace!("Getting dynamic properties for classes: {:?}", classes);
                        let result = (|| -> Result<HashMap<String, HashMap<String, Property>>, KnowledgeBaseError> { kb.get_dynamic_properties(classes) })();

                        let _ = reply.send(result);
                    }
                    KBCommand::AddClass(object_id, class_name, reply) => {
                        trace!("Adding class '{}' to object '{}'", class_name, object_id);

                        let result = (|| -> Result<(), KnowledgeBaseError> {
                            let static_props = kb.get_static_properties(HashSet::from([class_name.clone()]))?;
                            let dynamic_props = kb.get_dynamic_properties(HashSet::from([class_name.clone()]))?;

                            let object = kb.objects.get_mut(&object_id).ok_or_else(|| KnowledgeBaseError::ObjectNotFound(object_id.to_owned()))?;
                            object.classes.insert(class_name.clone());

                            for (class_name, props) in static_props {
                                let class = kb.classes.get(&class_name).ok_or_else(|| KnowledgeBaseError::ClassNotFound(format!("Class {} not found for object {}", class_name, object_id)))?;
                                for (name, prop) in props {
                                    let fb = kb.env.fact_builder(&format!("{}_{}", class.name, name)).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact builder for property {} of object {}: {}", name, object_id, e)))?;
                                    let fb = fb.put_symbol("id", object_id.as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set id slot for property {} of object {}: {}", name, object_id, e)))?;
                                    if let Some(v) = object.properties.as_ref().and_then(|props| props.get(&name)) {
                                        let fb: FactBuilder = set_prop(&kb.env, fb, &prop, v.clone(), None).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set property {} for object {}: {:#?}", name, object_id, e)))?;
                                        let fact = kb.env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for property {} of object {}: {}", name, object_id, e)))?;
                                        kb.values.entry(class.name.clone()).or_default().entry(object_id.clone()).or_default().insert(name.clone(), fact);
                                    } else {
                                        let def = get_default(&prop);
                                        let fb = set_prop(&kb.env, fb, &prop, def.clone(), None).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set default value for property {} of object {}: {:#?}", name, object_id, e)))?;
                                        let fact = kb.env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for default value of property {} of object {}: {}", name, object_id, e)))?;
                                        kb.values.entry(class.name.clone()).or_default().entry(object_id.clone()).or_default().insert(name.clone(), fact);
                                    }
                                }
                            }

                            for (class_name, props) in dynamic_props {
                                let class = kb.classes.get(&class_name).ok_or_else(|| KnowledgeBaseError::ClassNotFound(format!("Class {} not found for object {}", class_name, object_id)))?;
                                for (name, prop) in props {
                                    if let Some(v) = object.values.as_ref().and_then(|vals| vals.get(&name)) {
                                        let fb = kb.env.fact_builder(&format!("{}_{}", class.name, name)).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact builder for dynamic property {} of object {}: {}", name, object_id, e)))?;
                                        let fb = set_prop(&kb.env, fb, &prop, v.value.clone(), Some(v.timestamp)).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to set dynamic property {} for object {}: {:#?}", name, object_id, e)))?;
                                        let fact = kb.env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for dynamic property {} of object {}: {}", name, object_id, e)))?;
                                        kb.values.entry(class.name.clone()).or_default().entry(object_id.to_owned()).or_default().insert(name.clone(), fact);
                                    } else {
                                        let def = get_default(&prop);
                                        let fb = kb.env.fact_builder(&format!("{}_{}", class.name, name)).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact builder for dynamic property {} of object {}: {}", name, object_id, e)))?;
                                        let fb = set_prop(&kb.env, fb, &prop, def.clone(), Some(Utc::now()))?;
                                        let fact = kb.env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for default value of dynamic property {} of object {}: {}", name, object_id, e)))?;
                                        kb.values.entry(class.name.clone()).or_default().entry(object_id.to_owned()).or_default().insert(name.clone(), fact);
                                    }
                                }
                            }

                            let _ = event_tx.blocking_send(KnowledgeBaseEvent::AddedClass(object_id.clone(), class_name.clone()));
                            Ok(())
                        })();

                        let _ = reply.send(result);
                    }
                    KBCommand::GetObjectClasses(object_id, reply) => {
                        trace!("Getting classes for object '{}'", object_id);
                        let result = (|| -> Result<HashSet<String>, KnowledgeBaseError> {
                            let object = kb.objects.get(&object_id).ok_or_else(|| KnowledgeBaseError::ObjectNotFound(object_id.to_owned()))?;
                            kb.get_object_classes(object)
                        })();

                        let _ = reply.send(result);
                    }
                    KBCommand::SetProperties(object_id, properties, reply) => {
                        trace!("Setting properties for object '{}': {:?}", object_id, properties);
                        let result = (|| -> Result<(), KnowledgeBaseError> {
                            let static_props = kb.get_static_properties(kb.objects.get(&object_id).ok_or_else(|| KnowledgeBaseError::ObjectNotFound(object_id.to_owned()))?.classes.iter().cloned().collect())?;
                            let object = kb.objects.get_mut(&object_id).ok_or_else(|| KnowledgeBaseError::ObjectNotFound(object_id.to_owned()))?;
                            for (class_name, props) in static_props {
                                for (name, prop) in props {
                                    if let Some(v) = properties.get(&name) {
                                        object.properties.get_or_insert_with(HashMap::new).insert(name.clone(), v.clone());
                                        let fact = kb.values.get(&class_name).and_then(|objs| objs.get(&object_id)).and_then(|props| props.get(&name)).ok_or_else(|| KnowledgeBaseError::ObjectNotFound(format!("Fact for property {} of object {} of class {} not found", name, object_id, class_name)))?;
                                        let fm = kb.env.fact_modifier(fact).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact modifier for object {}: {}", object_id, e)))?;
                                        let fm = update_prop(&kb.env, fm, &prop, v.clone(), None)?;
                                        kb.env.modify_fact(fm).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to modify fact for property {} of object {}: {}", name, object_id, e)))?;
                                    }
                                }
                            }

                            let _ = event_tx.blocking_send(KnowledgeBaseEvent::UpdatedProperties(object_id.clone(), properties.clone()));
                            Ok(())
                        })();

                        let _ = reply.send(result);
                    }
                    KBCommand::AddValues(object_id, values, timestamp, reply) => {
                        trace!("Adding values for object '{}': {:?} at {}", object_id, values, timestamp);
                        let result = (|| -> Result<(), KnowledgeBaseError> {
                            let dynamic_props = kb.get_dynamic_properties(kb.objects.get(&object_id).ok_or_else(|| KnowledgeBaseError::ObjectNotFound(object_id.to_owned()))?.classes.iter().cloned().collect())?;
                            let object = kb.objects.get_mut(&object_id).ok_or_else(|| KnowledgeBaseError::ObjectNotFound(object_id.to_owned()))?;
                            for (class_name, props) in dynamic_props {
                                for (name, prop) in props {
                                    if let Some(v) = values.get(&name) {
                                        object.values.get_or_insert_with(HashMap::new).insert(name.clone(), TimedValue { value: v.clone(), timestamp });
                                        let fact = kb.values.get(&class_name).and_then(|objs| objs.get(&object_id)).and_then(|props| props.get(&name)).ok_or_else(|| KnowledgeBaseError::ObjectNotFound(format!("Fact for dynamic property {} of object {} of class {} not found", name, object_id, class_name)))?;
                                        let fm = kb.env.fact_modifier(fact).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact modifier for object {}: {}", object_id, e)))?;
                                        let fm = update_prop(&kb.env, fm, &prop, v.clone(), Some(timestamp))?;
                                        kb.env.modify_fact(fm).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to modify fact for dynamic property {} of object {}: {}", name, object_id, e)))?;
                                    }
                                }
                            }

                            let _ = event_tx.blocking_send(KnowledgeBaseEvent::AddedValues(object_id.clone(), values.clone(), timestamp));
                            Ok(())
                        })();

                        let _ = reply.send(result);
                    }
                    KBCommand::Build(construct, reply) => {
                        trace!("Building construct: {}", construct);
                        let result = (|| -> Result<(), KnowledgeBaseError> {
                            kb.env.build(construct.as_str()).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to build construct in CLIPS: {}", e)))?;
                            Ok(())
                        })();

                        let _ = reply.send(result);
                    }
                    KBCommand::AddUDF(name, return_type, min_args, max_args, arg_types, func, reply) => {
                        trace!("Adding UDF '{}'", name);
                        let result = (|| -> Result<(), KnowledgeBaseError> {
                            kb.env.add_udf(&name, return_type, min_args, max_args, arg_types, func).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to add UDF {}: {}", name, e)))?;
                            Ok(())
                        })();

                        let _ = reply.send(result);
                    }
                    KBCommand::AssertFact(template, fields, reply) => {
                        trace!("Asserting fact for template '{}'", template);
                        let result = (|| -> Result<u64, KnowledgeBaseError> {
                            let fb = kb.env.fact_builder(&template).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact builder for template {}: {}", template, e)))?;
                            let fb = fields.iter().try_fold(fb, |fb, (slot, value)| set_value(&kb.env, fb, slot, value))?;
                            let fact = kb.env.assert_fact(fb).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to assert fact for template {}: {}", template, e)))?;
                            let id = kb.next_fact_id;
                            kb.next_fact_id += 1;
                            kb.external_facts.insert(id, fact);
                            Ok(id)
                        })();
                        let _ = reply.send(result);
                    }
                    KBCommand::ModifyFact(fact_id, fields, reply) => {
                        trace!("Modifying fact {}", fact_id);
                        let result = (|| -> Result<(), KnowledgeBaseError> {
                            let fact = kb.external_facts.get(&fact_id).ok_or_else(|| KnowledgeBaseError::KBError(format!("External fact {} not found", fact_id)))?;
                            let fm = kb.env.fact_modifier(fact).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to create fact modifier for fact {}: {}", fact_id, e)))?;
                            let fm = fields.iter().try_fold(fm, |fm, (slot, value)| update_value(&kb.env, fm, slot, value))?;
                            kb.env.modify_fact(fm).map_err(|e| KnowledgeBaseError::KBError(format!("Failed to modify fact {}: {}", fact_id, e)))?;
                            Ok(())
                        })();
                        let _ = reply.send(result);
                    }
                }
            }
        });

        Self { tx, event_rx: Arc::new(Mutex::new(Some(event_rx))) }
    }

    pub async fn build(&self, construct: &str) -> Result<(), KnowledgeBaseError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.send(KBCommand::Build(construct.to_owned(), reply_tx)).await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to send Build command: {}", e)))?;
        reply_rx.await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to receive response for Build command: {}", e)))?
    }

    pub async fn add_udf(&self, name: &str, return_type: Option<Type>, min_args: u16, max_args: u16, arg_types: Vec<Type>, func: Udf) -> Result<(), KnowledgeBaseError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.send(KBCommand::AddUDF(name.to_owned(), return_type, min_args, max_args, arg_types, func, reply_tx)).await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to send AddUDF command: {}", e)))?;
        reply_rx.await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to receive response for AddUDF command: {}", e)))?
    }

    pub async fn assert_fact(&self, template: &str, fields: HashMap<String, Value>) -> Result<u64, KnowledgeBaseError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.send(KBCommand::AssertFact(template.to_owned(), fields, reply_tx)).await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to send AssertFact command: {}", e)))?;
        reply_rx.await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to receive response for AssertFact command: {}", e)))?
    }

    pub async fn modify_fact(&self, fact_id: u64, fields: HashMap<String, Value>) -> Result<(), KnowledgeBaseError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.send(KBCommand::ModifyFact(fact_id, fields, reply_tx)).await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to send ModifyFact command: {}", e)))?;
        reply_rx.await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to receive response for ModifyFact command: {}", e)))?
    }
}

#[async_trait]
impl KnowledgeBase for CLIPSKnowledgeBase {
    async fn create_class(&self, class: Class) -> Result<(), KnowledgeBaseError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.send(KBCommand::CreateClass(class, reply_tx)).await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to send CreateClass command: {}", e)))?;
        reply_rx.await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to receive response for CreateClass command: {}", e)))?
    }
    async fn get_static_properties(&self, classe_names: HashSet<String>) -> Result<HashMap<String, HashMap<String, Property>>, KnowledgeBaseError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.send(KBCommand::GetStaticProperties(classe_names, reply_tx)).await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to send GetStaticProperties command: {}", e)))?;
        reply_rx.await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to receive response for GetStaticProperties command: {}", e)))?
    }
    async fn get_dynamic_properties(&self, classe_names: HashSet<String>) -> Result<HashMap<String, HashMap<String, Property>>, KnowledgeBaseError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.send(KBCommand::GetDynamicProperties(classe_names, reply_tx)).await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to send GetDynamicProperties command: {}", e)))?;
        reply_rx.await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to receive response for GetDynamicProperties command: {}", e)))?
    }

    async fn create_rule(&self, rule: Rule) -> Result<(), KnowledgeBaseError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.send(KBCommand::CreateRule(rule, reply_tx)).await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to send CreateRule command: {}", e)))?;
        reply_rx.await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to receive response for CreateRule command: {}", e)))?
    }

    async fn create_object(&self, object: Object) -> Result<(), KnowledgeBaseError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.send(KBCommand::CreateObject(object, reply_tx)).await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to send CreateObject command: {}", e)))?;
        reply_rx.await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to receive response for CreateObject command: {}", e)))?
    }
    async fn add_class(&self, object_id: String, class_name: String) -> Result<(), KnowledgeBaseError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.send(KBCommand::AddClass(object_id, class_name, reply_tx)).await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to send AddClass command: {}", e)))?;
        reply_rx.await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to receive response for AddClass command: {}", e)))?
    }
    async fn get_object_classes(&self, object_id: String) -> Result<HashSet<String>, KnowledgeBaseError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.send(KBCommand::GetObjectClasses(object_id, reply_tx)).await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to send GetObjectClasses command: {}", e)))?;
        reply_rx.await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to receive response for GetObjectClasses command: {}", e)))?
    }
    async fn set_properties(&self, object_id: String, properties: HashMap<String, Value>) -> Result<(), KnowledgeBaseError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.send(KBCommand::SetProperties(object_id, properties.clone(), reply_tx)).await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to send SetProperties command: {}", e)))?;
        reply_rx.await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to receive response for SetProperties command: {}", e)))?
    }
    async fn add_values(&self, object_id: String, values: HashMap<String, Value>, timestamp: DateTime<Utc>) -> Result<(), KnowledgeBaseError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.send(KBCommand::AddValues(object_id, values.clone(), timestamp, reply_tx)).await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to send AddValues command: {}", e)))?;
        reply_rx.await.map_err(|e| KnowledgeBaseError::KBError(format!("Failed to receive response for AddValues command: {}", e)))?
    }

    fn take_event_receiver(&mut self) -> Option<mpsc::Receiver<KnowledgeBaseEvent>> {
        let mut guard = self.event_rx.lock().unwrap();
        guard.take()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kb::KnowledgeBase;
    use chrono::Utc;
    use std::collections::{HashMap, HashSet};

    fn mk_class(name: &str) -> Class {
        Class { name: name.to_owned(), parents: None, static_properties: None, dynamic_properties: None }
    }

    #[tokio::test]
    async fn create_class_succeeds_and_duplicate_fails() {
        let kb = CLIPSKnowledgeBase::new();
        let class = mk_class("sensor");

        kb.create_class(class.clone()).await.expect("class creation should succeed");

        let err = kb.create_class(class).await.expect_err("duplicate class creation should fail");

        assert!(matches!(err, KnowledgeBaseError::ClassAlreadyExists(name) if name == "sensor"));
    }

    #[tokio::test]
    async fn create_rule_succeeds_and_duplicate_fails() {
        let kb = CLIPSKnowledgeBase::new();
        let rule = Rule {
            name: "always-true".to_owned(),
            content: "(defrule always-true => (assert (rule-fired always-true)))".to_owned(),
        };

        kb.create_rule(rule.clone()).await.expect("rule creation should succeed");

        let err = kb.create_rule(rule).await.expect_err("duplicate rule creation should fail");

        assert!(matches!(err, KnowledgeBaseError::RuleAlreadyExists(name) if name == "always-true"));
    }

    #[tokio::test]
    async fn create_object_without_id_fails() {
        let kb = CLIPSKnowledgeBase::new();

        let err = kb.create_object(Object { id: None, classes: HashSet::from(["sensor".to_owned()]), properties: None, values: None }).await.expect_err("object without id should fail");

        assert!(matches!(err, KnowledgeBaseError::CreationError(_)));
    }

    #[tokio::test]
    async fn create_object_with_nonexistent_class_fails() {
        let kb = CLIPSKnowledgeBase::new();

        let err = kb
            .create_object(Object {
                id: Some("obj1".to_owned()),
                classes: HashSet::from(["missing".to_owned()]),
                properties: None,
                values: None,
            })
            .await
            .expect_err("object with nonexistent class should fail");

        assert!(matches!(err, KnowledgeBaseError::ClassNotFound(_)));
    }

    #[tokio::test]
    async fn create_object_succeeds_and_duplicate_fails() {
        let kb = CLIPSKnowledgeBase::new();

        kb.create_class(mk_class("sensor")).await.expect("class creation should succeed");

        let object = Object {
            id: Some("sensor1".to_owned()),
            classes: HashSet::from(["sensor".to_owned()]),
            properties: None,
            values: None,
        };

        kb.create_object(object.clone()).await.expect("object creation should succeed");

        let err = kb.create_object(object).await.expect_err("duplicate object creation should fail");

        assert!(matches!(err, KnowledgeBaseError::ObjectAlreadyExists(id) if id == "sensor1"));
    }

    #[tokio::test]
    async fn add_class_to_nonexistent_object_fails() {
        let kb = CLIPSKnowledgeBase::new();
        kb.create_class(mk_class("class1")).await.expect("class creation should succeed");

        let err = kb.add_class("missing-object".to_owned(), "class1".to_owned()).await.expect_err("add_class should fail for missing object");

        assert!(matches!(err, KnowledgeBaseError::ObjectNotFound(_)));
    }

    #[tokio::test]
    async fn add_nonexistent_class_to_object_fails() {
        let kb = CLIPSKnowledgeBase::new();
        kb.create_class(mk_class("class1")).await.expect("class creation should succeed");
        kb.create_object(Object {
            id: Some("obj1".to_owned()),
            classes: HashSet::from(["class1".to_owned()]),
            properties: None,
            values: None,
        })
        .await
        .expect("object creation should succeed");

        let err = kb.add_class("obj1".to_owned(), "missing-class".to_owned()).await.expect_err("add_class should fail for missing class");

        assert!(matches!(err, KnowledgeBaseError::ClassNotFound(_)));
    }

    #[tokio::test]
    async fn set_properties_on_nonexistent_object_fails() {
        let kb = CLIPSKnowledgeBase::new();
        let err = kb.set_properties("missing-object".to_owned(), HashMap::from([("value".to_owned(), Value::Int(42))])).await.expect_err("set_properties should fail for missing object");

        assert!(matches!(err, KnowledgeBaseError::ObjectNotFound(_)));
    }

    #[tokio::test]
    async fn set_properties_on_object_succeeds() {
        let kb = CLIPSKnowledgeBase::new();
        kb.create_class(Class {
            name: "Configurable".to_owned(),
            parents: None,
            static_properties: Some(HashMap::from([("value".to_owned(), Property::Int { default: Some(0), min: None, max: None })])),
            dynamic_properties: None,
        })
        .await
        .expect("class creation should succeed");

        kb.create_object(Object {
            id: Some("config1".to_owned()),
            classes: HashSet::from(["Configurable".to_owned()]),
            properties: None,
            values: None,
        })
        .await
        .expect("object creation should succeed");

        kb.set_properties("config1".to_owned(), HashMap::from([("value".to_owned(), Value::Int(100))])).await.expect("set_properties should succeed");
    }

    #[tokio::test]
    async fn add_values_on_nonexistent_object_fails() {
        let kb = CLIPSKnowledgeBase::new();
        let err = kb.add_values("missing-object".to_owned(), HashMap::from([("measurement".to_owned(), Value::Float(1.0))]), Utc::now()).await.expect_err("add_values should fail for missing object");

        assert!(matches!(err, KnowledgeBaseError::ObjectNotFound(_)));
    }

    #[tokio::test]
    async fn add_values_to_object_succeeds() {
        let kb = CLIPSKnowledgeBase::new();
        kb.create_class(Class {
            name: "TimeSeries".to_owned(),
            parents: None,
            static_properties: None,
            dynamic_properties: Some(HashMap::from([("measurement".to_owned(), Property::Float { default: Some(0.0), min: None, max: None })])),
        })
        .await
        .expect("class creation should succeed");

        kb.create_object(Object {
            id: Some("ts1".to_owned()),
            classes: HashSet::from(["TimeSeries".to_owned()]),
            properties: None,
            values: None,
        })
        .await
        .expect("object creation should succeed");

        kb.add_values("ts1".to_owned(), HashMap::from([("measurement".to_owned(), Value::Float(42.5))]), Utc::now()).await.expect("add_values should succeed");
    }

    #[test]
    fn get_default_bool_with_and_without_default() {
        assert_eq!(get_default(&Property::Bool { default: Some(true) }), Value::Bool(true));
        assert_eq!(get_default(&Property::Bool { default: None }), Value::Null);
    }

    #[test]
    fn prop_deftemplate_checks() {
        let class = mk_class("TestClass");

        let t1 = prop_deftemplate(&class, "active", &Property::Bool { default: Some(false) }, true);
        assert!(t1.contains("TestClass_active"));
        assert!(t1.contains("allowed-symbols TRUE FALSE nil"));

        let t2 = prop_deftemplate(&class, "percentage", &Property::Int { default: Some(50), min: Some(0), max: Some(100) }, true);
        assert!(t2.contains("range 0 100"));

        let t3 = prop_deftemplate(&class, "metric", &Property::Float { default: Some(1.5), min: None, max: None }, false);
        assert!(t3.contains("slot time"));
    }

    #[tokio::test]
    async fn complex_workflow_threshold_rule_setup_and_updates() {
        let kb = CLIPSKnowledgeBase::new();

        kb.create_class(Class {
            name: "ThermometerMonitor".to_owned(),
            parents: None,
            static_properties: None,
            dynamic_properties: Some(HashMap::from([("temperature".to_owned(), Property::Float { default: Some(20.0), min: None, max: None })])),
        })
        .await
        .expect("class creation should succeed");

        kb.create_object(Object {
            id: Some("thermo1".to_owned()),
            classes: HashSet::from(["ThermometerMonitor".to_owned()]),
            properties: None,
            values: None,
        })
        .await
        .expect("object creation should succeed");

        kb.create_rule(Rule {
            name: "temperature_alert_rule".to_owned(),
            content: "(defrule temperature_alert_rule\n                (ThermometerMonitor_temperature (id ?id) (value ?temp&:(> ?temp 35)))\n                =>\n                (add-data ?id (create$ temperature) (create$ 99.9))\n            )".to_owned(),
        })
        .await
        .expect("rule creation should succeed");

        kb.add_values("thermo1".to_owned(), HashMap::from([("temperature".to_owned(), Value::Float(36.0))]), Utc::now()).await.expect("add_values should succeed");

        kb.add_values("thermo1".to_owned(), HashMap::from([("temperature".to_owned(), Value::Float(25.0))]), Utc::now()).await.expect("second add_values should succeed");
    }

    #[tokio::test]
    async fn complex_workflow_multiple_classes_and_objects() {
        let kb = CLIPSKnowledgeBase::new();

        for i in 0..5 {
            kb.create_class(Class {
                name: format!("Class{}", i),
                parents: None,
                static_properties: None,
                dynamic_properties: Some(HashMap::from([("measurement".to_owned(), Property::Float { default: Some(0.0), min: None, max: None })])),
            })
            .await
            .expect("class creation should succeed");
        }

        for i in 0..5 {
            let id = format!("obj{}", i);
            let class_name = format!("Class{}", i);
            kb.create_object(Object { id: Some(id.clone()), classes: HashSet::from([class_name]), properties: None, values: None }).await.expect("object creation should succeed");

            kb.add_values(id, HashMap::from([("measurement".to_owned(), Value::Float(i as f64))]), Utc::now()).await.expect("add_values should succeed");
        }
    }

    #[tokio::test]
    async fn complex_workflow_object_with_multiple_classes_and_updates() {
        let kb = CLIPSKnowledgeBase::new();

        kb.create_class(Class {
            name: "Configurable".to_owned(),
            parents: None,
            static_properties: Some(HashMap::from([("name".to_owned(), Property::String { default: Some("Unknown".to_owned()) })])),
            dynamic_properties: None,
        })
        .await
        .expect("class creation should succeed");

        kb.create_class(Class {
            name: "TimeSeries".to_owned(),
            parents: None,
            static_properties: None,
            dynamic_properties: Some(HashMap::from([("reading".to_owned(), Property::Float { default: Some(0.0), min: None, max: None })])),
        })
        .await
        .expect("class creation should succeed");

        kb.create_object(Object {
            id: Some("multi_obj".to_owned()),
            classes: HashSet::from(["Configurable".to_owned()]),
            properties: None,
            values: None,
        })
        .await
        .expect("object creation should succeed");

        kb.add_class("multi_obj".to_owned(), "TimeSeries".to_owned()).await.expect("add_class should succeed");

        kb.set_properties("multi_obj".to_owned(), HashMap::from([("name".to_owned(), Value::String("Temperature Sensor".to_owned()))])).await.expect("set_properties should succeed");

        kb.add_values("multi_obj".to_owned(), HashMap::from([("reading".to_owned(), Value::Float(23.5))]), Utc::now()).await.expect("add_values should succeed");
    }

    #[tokio::test]
    async fn create_object_considers_parent_classes_for_properties_and_values() {
        let kb = CLIPSKnowledgeBase::new();

        kb.create_class(Class {
            name: "ParentClass".to_owned(),
            parents: None,
            static_properties: Some(HashMap::from([("threshold".to_owned(), Property::Int { default: Some(10), min: None, max: None })])),
            dynamic_properties: Some(HashMap::from([("baseline".to_owned(), Property::Float { default: Some(0.0), min: None, max: None })])),
        })
        .await
        .expect("parent class creation should succeed");

        kb.create_class(Class {
            name: "ChildClass".to_owned(),
            parents: Some(HashSet::from(["ParentClass".to_owned()])),
            static_properties: Some(HashMap::from([("name".to_owned(), Property::String { default: Some("sensor".to_owned()) })])),
            dynamic_properties: Some(HashMap::from([("reading".to_owned(), Property::Float { default: Some(0.0), min: None, max: None })])),
        })
        .await
        .expect("child class creation should succeed");

        kb.create_object(Object {
            id: Some("obj-parented".to_owned()),
            classes: HashSet::from(["ChildClass".to_owned()]),
            properties: None,
            values: None,
        })
        .await
        .expect("object creation should succeed");

        kb.set_properties("obj-parented".to_owned(), HashMap::from([("threshold".to_owned(), Value::Int(42))])).await.expect("set_properties should also work for parent class properties");

        kb.add_values("obj-parented".to_owned(), HashMap::from([("baseline".to_owned(), Value::Float(12.5))]), Utc::now()).await.expect("add_values should also work for parent class dynamic properties");
    }
}
