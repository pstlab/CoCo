use crate::model::{Class, Object, Rule, Value};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::{collections::HashMap, fmt};

#[cfg(feature = "mongodb")]
pub mod mongodb;

#[derive(Debug)]
pub enum DatabaseError {
    ConnectionError(String),
    NotFound(String),
    Exists(String),
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatabaseError::ConnectionError(msg) => write!(f, "Connection error: {}", msg),
            DatabaseError::NotFound(msg) => write!(f, "Not found: {}", msg),
            DatabaseError::Exists(msg) => write!(f, "Already exists: {}", msg),
        }
    }
}

// Il Database deve essere asincrono e clonabile
#[async_trait]
pub trait Database: Clone + Send + Sync + 'static {
    fn name(&self) -> &str;

    async fn get_classes(&self) -> Result<Vec<Class>, DatabaseError>;
    async fn get_class(&self, name: &str) -> Result<Option<Class>, DatabaseError>;
    async fn create_class(&self, class: Class) -> Result<(), DatabaseError>;

    async fn get_rules(&self) -> Result<Vec<Rule>, DatabaseError>;
    async fn get_rule(&self, name: &str) -> Result<Option<Rule>, DatabaseError>;
    async fn create_rule(&self, rule: Rule) -> Result<(), DatabaseError>;

    async fn get_objects(&self) -> Result<Vec<Object>, DatabaseError>;
    async fn get_object(&self, object_id: String) -> Result<Option<Object>, DatabaseError>;
    async fn create_object(&self, object: Object) -> Result<String, DatabaseError>;
    async fn add_class(&self, object_id: String, class_name: String) -> Result<(), DatabaseError>;
    async fn set_properties(&self, object_id: String, properties: &HashMap<String, Value>) -> Result<(), DatabaseError>;
    async fn add_values(&self, object_id: String, values: HashMap<String, Value>, date_time: DateTime<Utc>) -> Result<(), DatabaseError>;
    async fn get_values(&self, object_id: String, start_time: Option<DateTime<Utc>>, end_time: Option<DateTime<Utc>>) -> Result<Vec<(HashMap<String, Value>, DateTime<Utc>)>, DatabaseError>;

    async fn drop_database(&self) -> Result<(), DatabaseError>;
}
