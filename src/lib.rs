use crate::{
    db::Database,
    kb::{KnowledgeBase, KnowledgeBaseEvent},
    model::{CoCoClass, CoCoError, CoCoEvent, CoCoObject, CoCoProperty, CoCoRule, CoCoValue},
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{error, info, trace};

pub mod db;
#[cfg(feature = "fcm")]
pub mod fcm;
pub mod kb;
pub mod model;
#[cfg(feature = "mqtt")]
pub mod mqtt;
#[cfg(feature = "server")]
pub mod server;

type CommandResult<T> = oneshot::Sender<Result<T, CoCoError>>;
type Pulse = (HashMap<String, CoCoValue>, DateTime<Utc>);

#[async_trait]
pub trait CoCoModule<DB: Database, KB: KnowledgeBase>: Send + Sync {
    async fn init(&self, db: DB, kb: KB, coco: CoCo) -> Result<(), CoCoError>;
}

#[derive(Debug)]
enum CoCoCommand {
    Init(Vec<CoCoClass>, Vec<CoCoRule>, Vec<CoCoObject>, CommandResult<()>),
    GetClasses(CommandResult<Vec<CoCoClass>>),
    GetClass(String, CommandResult<Option<CoCoClass>>),
    GetStaticProperties(HashSet<String>, CommandResult<HashMap<String, HashMap<String, CoCoProperty>>>),
    GetDynamicProperties(HashSet<String>, CommandResult<HashMap<String, HashMap<String, CoCoProperty>>>),
    CreateClass(CoCoClass, CommandResult<()>),
    GetRules(CommandResult<Vec<CoCoRule>>),
    GetRule(String, CommandResult<Option<CoCoRule>>),
    CreateRule(CoCoRule, CommandResult<()>),
    GetObjects(CommandResult<Vec<CoCoObject>>),
    GetObject(String, CommandResult<Option<CoCoObject>>),
    GetObjectClasses(String, CommandResult<HashSet<String>>),
    CreateObject(CoCoObject, CommandResult<String>),
    AddClass(String, String, CommandResult<()>),
    SetProperties(String, HashMap<String, CoCoValue>, CommandResult<()>),
    AddValues(String, HashMap<String, CoCoValue>, DateTime<Utc>, CommandResult<()>),
    GetValues(String, Option<DateTime<Utc>>, Option<DateTime<Utc>>, CommandResult<Vec<Pulse>>),
}

#[derive(Clone)]
pub struct CoCo {
    tx: mpsc::Sender<CoCoCommand>,
    pub event_tx: broadcast::Sender<CoCoEvent>,
}

impl CoCo {
    pub async fn new<DB, KB>(db: DB, kb: KB, kb_event: mpsc::UnboundedReceiver<KnowledgeBaseEvent>, modules: Vec<Box<dyn CoCoModule<DB, KB>>>) -> Self
    where
        DB: Database,
        KB: KnowledgeBase,
    {
        let (command_tx, mut command_rx) = mpsc::channel::<CoCoCommand>(100);
        let (event_tx, _) = broadcast::channel(100);

        // Spawn a task to listen for events from the KnowledgeBase and forward them to CoCo's event channel
        let mut event_rx = kb_event;
        let event_tx_for_kb = event_tx.clone();
        let event_db = db.clone();
        let event_kb = kb.clone();
        tokio::spawn(async move {
            trace!("Starting task to listen for KnowledgeBase events");
            while let Some(event) = event_rx.recv().await {
                match event {
                    KnowledgeBaseEvent::AddedClass(object_id, class_name) => {
                        if let Err(e) = event_db.add_class(object_id.clone(), class_name).await {
                            error!("Failed to add class to database: {}", e);
                        }

                        match event_kb.get_object_classes(object_id.clone()).await {
                            Ok(classes) => {
                                let _ = event_tx_for_kb.send(CoCoEvent::ClassesUpdated(object_id, classes));
                            }
                            Err(e) => error!("Failed to load effective classes for object '{}': {}", object_id, e),
                        }
                    }
                    KnowledgeBaseEvent::UpdatedProperties(object_id, properties) if let Err(e) = event_db.set_properties(object_id.clone(), &properties).await => {
                        error!("Failed to update properties in database: {}", e);
                    }
                    KnowledgeBaseEvent::UpdatedProperties(object_id, properties) => {
                        let _ = event_tx_for_kb.send(CoCoEvent::PropertiesUpdated(object_id, properties));
                    }
                    KnowledgeBaseEvent::AddedValues(object_id, values, date_time) if let Err(e) = event_db.add_values(object_id.clone(), values.clone(), date_time).await => {
                        error!("Failed to add values to database: {}", e);
                    }
                    KnowledgeBaseEvent::AddedValues(object_id, values, date_time) => {
                        let _ = event_tx_for_kb.send(CoCoEvent::ValuesAdded(object_id, values, date_time));
                    }
                }
            }
        });

        // Spawn a task to listen for commands from CoCo's command channel and forward them to the KnowledgeBase
        let event_tx_for_commands = event_tx.clone();
        let command_db = db.clone();
        let command_kb = kb.clone();
        tokio::spawn(async move {
            trace!("Starting task to listen for CoCo commands");
            while let Some(command) = command_rx.recv().await {
                match command {
                    CoCoCommand::Init(classes, rules, objects, response_tx) => {
                        for class in classes {
                            match command_kb.create_class(class.clone()).await {
                                Ok(_) => {
                                    let _ = event_tx_for_commands.send(CoCoEvent::ClassCreated(class.name.clone()));
                                }
                                Err(e) => {
                                    let _ = response_tx.send(Err(CoCoError::KnowledgeBaseError(e.to_string())));
                                    return;
                                }
                            }
                        }
                        for rule in rules {
                            match command_kb.create_rule(rule.clone()).await {
                                Ok(_) => {
                                    let _ = event_tx_for_commands.send(CoCoEvent::RuleCreated(rule.name.clone()));
                                }
                                Err(e) => {
                                    let _ = response_tx.send(Err(CoCoError::KnowledgeBaseError(e.to_string())));
                                    return;
                                }
                            }
                        }
                        for object in objects {
                            match command_kb.create_object(object.clone()).await {
                                Ok(_) => {
                                    let _ = event_tx_for_commands.send(CoCoEvent::ObjectCreated(object.id.clone().unwrap_or_default()));
                                }
                                Err(e) => {
                                    let _ = response_tx.send(Err(CoCoError::KnowledgeBaseError(e.to_string())));
                                    return;
                                }
                            }
                        }
                        let _ = response_tx.send(Ok(()));
                    }
                    CoCoCommand::GetClasses(response_tx) => {
                        let classes = command_kb.get_classes().await.map_err(|e| CoCoError::DatabaseError(e.to_string()));
                        let _ = response_tx.send(classes);
                    }
                    CoCoCommand::GetClass(class_name, response_tx) => {
                        let class = command_kb.get_class(&class_name).await.map_err(|e| CoCoError::DatabaseError(e.to_string()));
                        let _ = response_tx.send(class);
                    }
                    CoCoCommand::GetStaticProperties(classe_names, response_tx) => {
                        let properties = command_kb.get_static_properties(classe_names).await.map_err(|e| CoCoError::DatabaseError(e.to_string()));
                        let _ = response_tx.send(properties);
                    }
                    CoCoCommand::GetDynamicProperties(classe_names, response_tx) => {
                        let properties = command_kb.get_dynamic_properties(classe_names).await.map_err(|e| CoCoError::DatabaseError(e.to_string()));
                        let _ = response_tx.send(properties);
                    }
                    CoCoCommand::CreateClass(class, response_tx) => {
                        let class_name = class.name.clone();
                        let result = async {
                            command_kb.create_class(class.clone()).await.map_err(|e| CoCoError::KnowledgeBaseError(e.to_string()))?;
                            command_db.create_class(class).await.map_err(|e| CoCoError::DatabaseError(e.to_string()))?;
                            Ok::<(), CoCoError>(())
                        }
                        .await;

                        if result.is_ok() {
                            let _ = event_tx_for_commands.send(CoCoEvent::ClassCreated(class_name));
                        }
                        let _ = response_tx.send(result);
                    }
                    CoCoCommand::GetRules(response_tx) => {
                        let rules = command_kb.get_rules().await.map_err(|e| CoCoError::DatabaseError(e.to_string()));
                        let _ = response_tx.send(rules);
                    }
                    CoCoCommand::GetRule(rule_name, response_tx) => {
                        let rule = command_kb.get_rule(&rule_name).await.map_err(|e| CoCoError::DatabaseError(e.to_string()));
                        let _ = response_tx.send(rule);
                    }
                    CoCoCommand::CreateRule(rule, response_tx) => {
                        let rule_name = rule.name.clone();
                        let result = async {
                            command_kb.create_rule(rule.clone()).await.map_err(|e| CoCoError::KnowledgeBaseError(e.to_string()))?;
                            command_db.create_rule(rule).await.map_err(|e| CoCoError::DatabaseError(e.to_string()))?;
                            Ok::<(), CoCoError>(())
                        }
                        .await;
                        if result.is_ok() {
                            let _ = event_tx_for_commands.send(CoCoEvent::RuleCreated(rule_name));
                        }
                        let _ = response_tx.send(result);
                    }
                    CoCoCommand::GetObjects(response_tx) => {
                        let objects = command_kb.get_objects().await.map_err(|e| CoCoError::DatabaseError(e.to_string()));
                        let _ = response_tx.send(objects);
                    }
                    CoCoCommand::GetObject(object_id, response_tx) => {
                        let object = command_kb.get_object(object_id).await.map_err(|e| CoCoError::DatabaseError(e.to_string()));
                        let _ = response_tx.send(object);
                    }
                    CoCoCommand::GetObjectClasses(object_id, response_tx) => {
                        let classes = command_kb.get_object_classes(object_id).await.map_err(|e| CoCoError::DatabaseError(e.to_string()));
                        let _ = response_tx.send(classes);
                    }
                    CoCoCommand::CreateObject(object, response_tx) => {
                        let result = async {
                            let id = command_db.create_object(object.clone()).await.map_err(|e| CoCoError::DatabaseError(e.to_string()))?;
                            let object = CoCoObject { id: Some(id.clone()), ..object };
                            command_kb.create_object(object).await.map_err(|e| CoCoError::KnowledgeBaseError(e.to_string()))?;
                            Ok::<String, CoCoError>(id)
                        }
                        .await;
                        if result.is_ok() {
                            let _ = event_tx_for_commands.send(CoCoEvent::ObjectCreated(result.clone().unwrap()));
                        }
                        let _ = response_tx.send(result);
                    }
                    CoCoCommand::AddClass(object_id, class_name, response_tx) => {
                        let result = async {
                            command_kb.add_class(object_id.clone(), class_name.clone()).await.map_err(|e| CoCoError::KnowledgeBaseError(e.to_string()))?;
                            command_db.add_class(object_id.clone(), class_name.clone()).await.map_err(|e| CoCoError::DatabaseError(e.to_string()))?;
                            Ok::<(), CoCoError>(())
                        }
                        .await;
                        if result.is_ok()
                            && let Ok(classes) = command_kb.get_object_classes(object_id.clone()).await
                        {
                            let _ = event_tx_for_commands.send(CoCoEvent::ClassesUpdated(object_id, classes));
                        }
                        let _ = response_tx.send(result);
                    }
                    CoCoCommand::SetProperties(object_id, properties, response_tx) => {
                        let result = async {
                            command_kb.set_properties(object_id.clone(), properties.clone()).await.map_err(|e| CoCoError::KnowledgeBaseError(e.to_string()))?;
                            command_db.set_properties(object_id.clone(), &properties).await.map_err(|e| CoCoError::DatabaseError(e.to_string()))?;
                            Ok::<(), CoCoError>(())
                        }
                        .await;
                        if result.is_ok() {
                            let _ = event_tx_for_commands.send(CoCoEvent::PropertiesUpdated(object_id, properties));
                        }
                        let _ = response_tx.send(result);
                    }
                    CoCoCommand::AddValues(object_id, values, date_time, response_tx) => {
                        let result = async {
                            command_kb.add_values(object_id.clone(), values.clone(), date_time).await.map_err(|e| CoCoError::KnowledgeBaseError(e.to_string()))?;
                            command_db.add_values(object_id.clone(), values.clone(), date_time).await.map_err(|e| CoCoError::DatabaseError(e.to_string()))?;
                            Ok::<(), CoCoError>(())
                        }
                        .await;
                        if result.is_ok() {
                            let _ = event_tx_for_commands.send(CoCoEvent::ValuesAdded(object_id, values, date_time));
                        }
                        let _ = response_tx.send(result);
                    }
                    CoCoCommand::GetValues(object_id, start_time, end_time, response_tx) => {
                        let result = async {
                            let values = command_db.get_values(object_id.clone(), start_time, end_time).await.map_err(|e| CoCoError::DatabaseError(e.to_string()))?;
                            Ok::<Vec<(HashMap<String, CoCoValue>, DateTime<Utc>)>, CoCoError>(values)
                        }
                        .await;
                        let _ = response_tx.send(result);
                    }
                }
            }
        });

        info!("Loading classes, objects, and rules from database into knowledge base");
        let classes = db.get_classes().await.unwrap_or_else(|e| {
            error!("Error fetching classes from database: {:?}", e);
            vec![]
        });
        let rules = db.get_rules().await.unwrap_or_else(|e| {
            error!("Error fetching rules from database: {:?}", e);
            vec![]
        });
        let objects = db.get_objects().await.unwrap_or_else(|e| {
            error!("Error fetching objects from database: {:?}", e);
            vec![]
        });

        let coco = CoCo { tx: command_tx, event_tx };

        for module in modules {
            module.init(db.clone(), kb.clone(), coco.clone()).await.expect("Failed to initialize CoCo module");
        }

        coco.init(classes, rules, objects).await.expect("Failed to initialize CoCo with data from database");
        coco
    }

    async fn init(&self, classes: Vec<CoCoClass>, rules: Vec<CoCoRule>, objects: Vec<CoCoObject>) -> Result<(), CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::Init(classes, rules, objects, response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn get_classes(&self) -> Result<Vec<CoCoClass>, CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::GetClasses(response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn get_class(&self, name: String) -> Result<Option<CoCoClass>, CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::GetClass(name, response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn get_static_properties(&self, classe_names: HashSet<String>) -> Result<HashMap<String, HashMap<String, CoCoProperty>>, CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::GetStaticProperties(classe_names, response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn get_dynamic_properties(&self, classe_names: HashSet<String>) -> Result<HashMap<String, HashMap<String, CoCoProperty>>, CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::GetDynamicProperties(classe_names, response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn create_class(&self, class: CoCoClass) -> Result<(), CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::CreateClass(class, response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn get_rules(&self) -> Result<Vec<CoCoRule>, CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::GetRules(response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn get_rule(&self, name: String) -> Result<Option<CoCoRule>, CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::GetRule(name, response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn create_rule(&self, rule: CoCoRule) -> Result<(), CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::CreateRule(rule, response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn get_objects(&self) -> Result<Vec<CoCoObject>, CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::GetObjects(response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn get_object(&self, object_id: String) -> Result<Option<CoCoObject>, CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::GetObject(object_id, response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn get_object_classes(&self, object_id: String) -> Result<HashSet<String>, CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::GetObjectClasses(object_id, response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn create_object(&self, object: CoCoObject) -> Result<String, CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::CreateObject(object, response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn add_class(&self, object_id: String, class_name: String) -> Result<(), CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::AddClass(object_id, class_name, response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn set_properties(&self, object_id: String, properties: HashMap<String, CoCoValue>) -> Result<(), CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::SetProperties(object_id, properties.clone(), response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn add_values(&self, object_id: String, values: HashMap<String, CoCoValue>, date_time: DateTime<Utc>) -> Result<(), CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::AddValues(object_id, values.clone(), date_time, response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn get_values(&self, object_id: String, start_time: Option<DateTime<Utc>>, end_time: Option<DateTime<Utc>>) -> Result<Vec<(HashMap<String, CoCoValue>, DateTime<Utc>)>, CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::GetValues(object_id, start_time, end_time, response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }
}
