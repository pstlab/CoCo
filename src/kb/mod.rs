#[cfg(feature = "clips")]
use crate::kb::clips::CLIPSKnowledgeBase;
use crate::model::{Class, Object, Rule, Value};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::{collections::HashMap, fmt};
use tokio::sync::mpsc;

#[cfg(feature = "clips")]
pub mod clips;

#[derive(Debug)]
pub enum KnowledgeBaseError {
    CreationError(String),
    ClassAlreadyExists(String),
    ClassNotFound(String),
    ObjectAlreadyExists(String),
    ObjectNotFound(String),
    RuleAlreadyExists(String),
    RuleNotFound(String),
    KBError(String),
}

impl fmt::Display for KnowledgeBaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KnowledgeBaseError::CreationError(msg) => write!(f, "Creation error: {}", msg),
            KnowledgeBaseError::ClassAlreadyExists(name) => write!(f, "Class already exists: {}", name),
            KnowledgeBaseError::ClassNotFound(name) => write!(f, "Class not found: {}", name),
            KnowledgeBaseError::ObjectAlreadyExists(id) => write!(f, "Object already exists: {}", id),
            KnowledgeBaseError::ObjectNotFound(id) => write!(f, "Object not found: {}", id),
            KnowledgeBaseError::RuleAlreadyExists(name) => write!(f, "Rule already exists: {}", name),
            KnowledgeBaseError::RuleNotFound(name) => write!(f, "Rule not found: {}", name),
            KnowledgeBaseError::KBError(msg) => write!(f, "Knowledge base error: {}", msg),
        }
    }
}

#[derive(Debug)]
pub enum KnowledgeBaseEvent {
    AddedClass(String, String),                                 // (object_id, class_name)
    UpdatedProperties(String, HashMap<String, Value>),          // (object_id, properties)
    AddedValues(String, HashMap<String, Value>, DateTime<Utc>), // (object_id, value, date_time)
}

impl fmt::Display for KnowledgeBaseEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KnowledgeBaseEvent::AddedClass(object_id, class_name) => write!(f, "Added class '{}' to object '{}'", class_name, object_id),
            KnowledgeBaseEvent::UpdatedProperties(object_id, properties) => write!(f, "Updated properties for object '{}': {:?}", object_id, properties),
            KnowledgeBaseEvent::AddedValues(object_id, values, date_time) => write!(f, "Added values for object '{}': {:?} at {}", object_id, values, date_time),
        }
    }
}

#[async_trait]
pub trait KnowledgeBase: Clone + Send + Sync + 'static {
    async fn create_class(&self, class: Class) -> Result<(), KnowledgeBaseError>;

    async fn create_rule(&self, rule: Rule) -> Result<(), KnowledgeBaseError>;

    async fn create_object(&self, object: Object) -> Result<(), KnowledgeBaseError>;
    async fn add_class(&self, object_id: String, class_name: String) -> Result<(), KnowledgeBaseError>;
    async fn set_properties(&self, object_id: String, properties: HashMap<String, Value>) -> Result<(), KnowledgeBaseError>;
    async fn add_values(&self, object_id: String, values: HashMap<String, Value>, date_time: DateTime<Utc>) -> Result<(), KnowledgeBaseError>;

    fn take_event_receiver(&mut self) -> Option<mpsc::Receiver<KnowledgeBaseEvent>>;
}

pub fn setup_kb() -> Result<impl KnowledgeBase, KnowledgeBaseError> {
    #[cfg(feature = "clips")]
    return setup_clips();

    #[cfg(not(feature = "clips"))]
    panic!("No knowledge base backend configured");
}

#[cfg(feature = "clips")]
pub fn setup_clips() -> Result<CLIPSKnowledgeBase, KnowledgeBaseError> {
    Ok(CLIPSKnowledgeBase::new())
}
