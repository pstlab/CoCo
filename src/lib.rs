use crate::{
    db::Database,
    kb::{KnowledgeBase, KnowledgeBaseEvent},
    model::{Class, CoCoError, CoCoEvent, Object, Property, Rule, Value},
};
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
type Pulse = (HashMap<String, Value>, DateTime<Utc>);

#[derive(Debug)]
enum CoCoCommand {
    Init(Vec<Class>, Vec<Rule>, Vec<Object>, CommandResult<()>),
    GetClasses(CommandResult<Vec<Class>>),
    GetClass(String, CommandResult<Option<Class>>),
    GetStaticProperties(HashSet<String>, CommandResult<HashMap<String, HashMap<String, Property>>>),
    GetDynamicProperties(HashSet<String>, CommandResult<HashMap<String, HashMap<String, Property>>>),
    CreateClass(Class, CommandResult<()>),
    GetRules(CommandResult<Vec<Rule>>),
    GetRule(String, CommandResult<Option<Rule>>),
    CreateRule(Rule, CommandResult<()>),
    GetObjects(CommandResult<Vec<Object>>),
    GetObject(String, CommandResult<Option<Object>>),
    GetObjectClasses(String, CommandResult<HashSet<String>>),
    CreateObject(Object, CommandResult<String>),
    AddClass(String, String, CommandResult<()>),
    SetProperties(String, HashMap<String, Value>, CommandResult<()>),
    AddValues(String, HashMap<String, Value>, DateTime<Utc>, CommandResult<()>),
    GetValues(String, Option<DateTime<Utc>>, Option<DateTime<Utc>>, CommandResult<Vec<Pulse>>),
}

#[derive(Clone)]
pub struct CoCo {
    tx: mpsc::Sender<CoCoCommand>,
    pub event_tx: broadcast::Sender<CoCoEvent>,
}
impl CoCo {
    pub async fn new<DB, KB>(db: DB, mut kb: KB) -> Self
    where
        DB: Database,
        KB: KnowledgeBase,
    {
        let (command_tx, mut command_rx) = mpsc::channel::<CoCoCommand>(100);
        let (event_tx, _) = broadcast::channel(100);

        // Spawn a task to listen for events from the KnowledgeBase and forward them to CoCo's event channel
        let mut event_rx = kb.take_event_receiver().expect("KnowledgeBase must provide an event receiver");
        let event_tx_for_kb = event_tx.clone();
        let event_db = db.clone();
        tokio::spawn(async move {
            trace!("Starting task to listen for KnowledgeBase events");
            while let Some(event) = event_rx.recv().await {
                match event {
                    KnowledgeBaseEvent::AddedClass(object_id, class_name) if let Err(e) = event_db.add_class(object_id.clone(), class_name.clone()).await => {
                        error!("Failed to add class to database: {}", e);
                    }
                    KnowledgeBaseEvent::AddedClass(object_id, class_name) => {
                        let _ = event_tx_for_kb.send(CoCoEvent::AddedClass(object_id, class_name));
                    }
                    KnowledgeBaseEvent::UpdatedProperties(object_id, properties) if let Err(e) = event_db.set_properties(object_id.clone(), &properties).await => {
                        error!("Failed to update properties in database: {}", e);
                    }
                    KnowledgeBaseEvent::UpdatedProperties(object_id, properties) => {
                        let _ = event_tx_for_kb.send(CoCoEvent::UpdatedProperties(object_id, properties));
                    }
                    KnowledgeBaseEvent::AddedValues(object_id, values, date_time) if let Err(e) = event_db.add_values(object_id.clone(), values.clone(), date_time).await => {
                        error!("Failed to add values to database: {}", e);
                    }
                    KnowledgeBaseEvent::AddedValues(object_id, values, date_time) => {
                        let _ = event_tx_for_kb.send(CoCoEvent::AddedValues(object_id, values, date_time));
                    }
                }
            }
        });

        // Spawn a task to listen for commands from CoCo's command channel and forward them to the KnowledgeBase
        let event_tx_for_commands = event_tx.clone();
        let command_db = db.clone();
        tokio::spawn(async move {
            trace!("Starting task to listen for CoCo commands");
            while let Some(command) = command_rx.recv().await {
                match command {
                    CoCoCommand::Init(classes, rules, objects, response_tx) => {
                        for class in classes {
                            if let Err(e) = kb.create_class(class.clone()).await {
                                let _ = response_tx.send(Err(CoCoError::KnowledgeBaseError(e.to_string())));
                                return;
                            }
                        }
                        for rule in rules {
                            if let Err(e) = kb.create_rule(rule.clone()).await {
                                let _ = response_tx.send(Err(CoCoError::KnowledgeBaseError(e.to_string())));
                                return;
                            }
                        }
                        for object in objects {
                            if let Err(e) = kb.create_object(object.clone()).await {
                                let _ = response_tx.send(Err(CoCoError::KnowledgeBaseError(e.to_string())));
                                return;
                            }
                        }
                        let _ = response_tx.send(Ok(()));
                    }
                    CoCoCommand::GetClasses(response_tx) => {
                        let classes = command_db.get_classes().await.map_err(|e| CoCoError::DatabaseError(e.to_string()));
                        let _ = response_tx.send(classes);
                    }
                    CoCoCommand::GetClass(class_name, response_tx) => {
                        let class = command_db.get_class(&class_name).await.map_err(|e| CoCoError::DatabaseError(e.to_string()));
                        let _ = response_tx.send(class);
                    }
                    CoCoCommand::GetStaticProperties(classe_names, response_tx) => {
                        let properties = kb.get_static_properties(classe_names).await.map_err(|e| CoCoError::DatabaseError(e.to_string()));
                        let _ = response_tx.send(properties);
                    }
                    CoCoCommand::GetDynamicProperties(classe_names, response_tx) => {
                        let properties = kb.get_dynamic_properties(classe_names).await.map_err(|e| CoCoError::DatabaseError(e.to_string()));
                        let _ = response_tx.send(properties);
                    }
                    CoCoCommand::CreateClass(class, response_tx) => {
                        let class_name = class.name.clone();
                        let result = async {
                            kb.create_class(class.clone()).await.map_err(|e| CoCoError::KnowledgeBaseError(e.to_string()))?;
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
                        let rules = command_db.get_rules().await.map_err(|e| CoCoError::DatabaseError(e.to_string()));
                        let _ = response_tx.send(rules);
                    }
                    CoCoCommand::GetRule(rule_name, response_tx) => {
                        let rule = command_db.get_rule(&rule_name).await.map_err(|e| CoCoError::DatabaseError(e.to_string()));
                        let _ = response_tx.send(rule);
                    }
                    CoCoCommand::CreateRule(rule, response_tx) => {
                        let rule_name = rule.name.clone();
                        let result = async {
                            kb.create_rule(rule.clone()).await.map_err(|e| CoCoError::KnowledgeBaseError(e.to_string()))?;
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
                        let objects = command_db.get_objects().await.map_err(|e| CoCoError::DatabaseError(e.to_string()));
                        let _ = response_tx.send(objects);
                    }
                    CoCoCommand::GetObject(object_id, response_tx) => {
                        let object = command_db.get_object(object_id).await.map_err(|e| CoCoError::DatabaseError(e.to_string()));
                        let _ = response_tx.send(object);
                    }
                    CoCoCommand::GetObjectClasses(object_id, response_tx) => {
                        let classes = kb.get_object_classes(object_id).await.map_err(|e| CoCoError::DatabaseError(e.to_string()));
                        let _ = response_tx.send(classes);
                    }
                    CoCoCommand::CreateObject(object, response_tx) => {
                        let result = async {
                            let id = command_db.create_object(object.clone()).await.map_err(|e| CoCoError::DatabaseError(e.to_string()))?;
                            let object = Object { id: Some(id.clone()), ..object };
                            kb.create_object(object).await.map_err(|e| CoCoError::KnowledgeBaseError(e.to_string()))?;
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
                            kb.add_class(object_id.clone(), class_name.clone()).await.map_err(|e| CoCoError::KnowledgeBaseError(e.to_string()))?;
                            command_db.add_class(object_id.clone(), class_name.clone()).await.map_err(|e| CoCoError::DatabaseError(e.to_string()))?;
                            Ok::<(), CoCoError>(())
                        }
                        .await;
                        if result.is_ok() {
                            let _ = event_tx_for_commands.send(CoCoEvent::AddedClass(object_id, class_name));
                        }
                        let _ = response_tx.send(result);
                    }
                    CoCoCommand::SetProperties(object_id, properties, response_tx) => {
                        let result = async {
                            kb.set_properties(object_id.clone(), properties.clone()).await.map_err(|e| CoCoError::KnowledgeBaseError(e.to_string()))?;
                            command_db.set_properties(object_id.clone(), &properties).await.map_err(|e| CoCoError::DatabaseError(e.to_string()))?;
                            Ok::<(), CoCoError>(())
                        }
                        .await;
                        if result.is_ok() {
                            let _ = event_tx_for_commands.send(CoCoEvent::UpdatedProperties(object_id, properties));
                        }
                        let _ = response_tx.send(result);
                    }
                    CoCoCommand::AddValues(object_id, values, date_time, response_tx) => {
                        let result = async {
                            kb.add_values(object_id.clone(), values.clone(), date_time).await.map_err(|e| CoCoError::KnowledgeBaseError(e.to_string()))?;
                            command_db.add_values(object_id.clone(), values.clone(), date_time).await.map_err(|e| CoCoError::DatabaseError(e.to_string()))?;
                            Ok::<(), CoCoError>(())
                        }
                        .await;
                        if result.is_ok() {
                            let _ = event_tx_for_commands.send(CoCoEvent::AddedValues(object_id, values, date_time));
                        }
                        let _ = response_tx.send(result);
                    }
                    CoCoCommand::GetValues(object_id, start_time, end_time, response_tx) => {
                        let result = async {
                            let values = command_db.get_values(object_id.clone(), start_time, end_time).await.map_err(|e| CoCoError::DatabaseError(e.to_string()))?;
                            Ok::<Vec<(HashMap<String, Value>, DateTime<Utc>)>, CoCoError>(values)
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
        coco.init(classes, rules, objects).await.expect("Failed to initialize CoCo with data from database");
        coco
    }

    async fn init(&self, classes: Vec<Class>, rules: Vec<Rule>, objects: Vec<Object>) -> Result<(), CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::Init(classes, rules, objects, response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn get_classes(&self) -> Result<Vec<Class>, CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::GetClasses(response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn get_class(&self, name: &str) -> Result<Option<Class>, CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::GetClass(name.to_owned(), response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn get_static_properties(&self, classe_names: HashSet<String>) -> Result<HashMap<String, HashMap<String, Property>>, CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::GetStaticProperties(classe_names, response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn get_dynamic_properties(&self, classe_names: HashSet<String>) -> Result<HashMap<String, HashMap<String, Property>>, CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::GetDynamicProperties(classe_names, response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn create_class(&self, class: Class) -> Result<(), CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        let class_name = class.name.clone();
        self.tx.send(CoCoCommand::CreateClass(class, response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        let _ = response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?;
        self.event_tx.send(CoCoEvent::ClassCreated(class_name)).map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send event from CoCo: {}", e)))?;
        Ok(())
    }

    pub async fn get_rules(&self) -> Result<Vec<Rule>, CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::GetRules(response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn get_rule(&self, name: &str) -> Result<Option<Rule>, CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::GetRule(name.to_owned(), response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn create_rule(&self, rule: Rule) -> Result<(), CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        let rule_name = rule.name.clone();
        self.tx.send(CoCoCommand::CreateRule(rule, response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        let _ = response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?;
        self.event_tx.send(CoCoEvent::RuleCreated(rule_name)).map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send event from CoCo: {}", e)))?;
        Ok(())
    }

    pub async fn get_objects(&self) -> Result<Vec<Object>, CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::GetObjects(response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn get_object(&self, object_id: &str) -> Result<Option<Object>, CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::GetObject(object_id.to_owned(), response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn get_object_classes(&self, object_id: &str) -> Result<HashSet<String>, CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::GetObjectClasses(object_id.to_owned(), response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn create_object(&self, object: Object) -> Result<(), CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::CreateObject(object, response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        match response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))? {
            Ok(id) => {
                self.event_tx.send(CoCoEvent::ObjectCreated(id)).map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send event from CoCo: {}", e)))?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub async fn add_class(&self, object_id: &str, class_name: &str) -> Result<(), CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::AddClass(object_id.to_owned(), class_name.to_owned(), response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn set_properties(&self, object_id: &str, properties: HashMap<String, Value>) -> Result<(), CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::SetProperties(object_id.to_owned(), properties.clone(), response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn add_values(&self, object_id: &str, values: HashMap<String, Value>, date_time: DateTime<Utc>) -> Result<(), CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::AddValues(object_id.to_owned(), values.clone(), date_time, response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }

    pub async fn get_values(&self, object_id: &str, start_time: Option<DateTime<Utc>>, end_time: Option<DateTime<Utc>>) -> Result<Vec<(HashMap<String, Value>, DateTime<Utc>)>, CoCoError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(CoCoCommand::GetValues(object_id.to_owned(), start_time, end_time, response_tx)).await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to send command to CoCo: {}", e)))?;
        response_rx.await.map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to receive response from CoCo: {}", e)))?
    }
}
