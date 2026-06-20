use std::collections::{HashMap, HashSet};

use crate::db::{Database, DatabaseError};
use crate::model::{CoCoClass, CoCoObject, CoCoRule, CoCoValue, TimedValue};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use mongodb::bson::oid::ObjectId;
use mongodb::bson::{self, doc};
use mongodb::{Client, IndexModel, bson::Document, options::IndexOptions};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct MongoDB {
    name: String,
    pub client: Client,
}

#[derive(Serialize, Deserialize, Debug)]
struct MongoObject {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    pub classes: HashSet<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, MongoValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<HashMap<String, MongoTimedValue>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", content = "value")]
enum MongoValue {
    #[serde(rename = "null")]
    Null,
    #[serde(rename = "bool")]
    Bool(bool),
    #[serde(rename = "int")]
    Int(i64),
    #[serde(rename = "float")]
    Float(f64),
    #[serde(rename = "string")]
    String(String),
    #[serde(rename = "symbol")]
    Symbol(String),
    #[serde(rename = "object")]
    Object(String),
    #[serde(rename = "bool_array")]
    BoolArray(Vec<bool>),
    #[serde(rename = "int_array")]
    IntArray(Vec<i64>),
    #[serde(rename = "float_array")]
    FloatArray(Vec<f64>),
    #[serde(rename = "string_array")]
    StringArray(Vec<String>),
    #[serde(rename = "symbol_array")]
    SymbolArray(Vec<String>),
    #[serde(rename = "object_array")]
    ObjectArray(Vec<String>),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct MongoTimedValue {
    value: MongoValue,
    timestamp: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug)]
struct ObjectData {
    pub object_id: String,
    pub values: HashMap<String, MongoValue>,
    pub timestamp: DateTime<Utc>,
}

impl MongoDB {
    pub async fn new(name: String, connection_string: String) -> Result<Self, DatabaseError> {
        let client = Client::with_uri_str(connection_string).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        let db = client.database(&name);
        let collection_names = db.list_collection_names().await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        if collection_names.is_empty() {
            let classes_collection = db.collection::<Document>("classes");
            let index = IndexModel::builder().keys(doc! { "name": 1 }).options(IndexOptions::builder().unique(true).build()).build();
            classes_collection.create_index(index).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;

            let rules_collection = db.collection::<Document>("rules");
            let index = IndexModel::builder().keys(doc! { "name": 1 }).options(IndexOptions::builder().unique(true).build()).build();
            rules_collection.create_index(index).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;

            let object_data_collection = db.collection::<Document>("object_data");
            let index = IndexModel::builder().keys(doc! { "object_id": 1, "timestamp": 1 }).options(IndexOptions::builder().unique(true).build()).build();
            object_data_collection.create_index(index).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        }
        Ok(Self { name, client })
    }

    pub async fn default() -> Result<Self, DatabaseError> {
        let name: String = std::env::var("DB_NAME").unwrap_or_else(|_| "coco_db".to_owned());
        let host = std::env::var("DB_HOST").unwrap_or_else(|_| "localhost".to_owned());
        let port = std::env::var("DB_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(27017);
        let uri = format!("mongodb://{}:{}", host, port);
        Self::new(name, uri).await
    }
}

#[async_trait]
impl Database for MongoDB {
    fn name(&self) -> &str {
        &self.name
    }

    async fn get_classes(&self) -> Result<Vec<CoCoClass>, DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<CoCoClass>("classes");
        let cursor = collection.find(doc! {}).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        let classes: Vec<CoCoClass> = cursor.try_collect().await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        Ok(classes)
    }

    async fn create_class(&self, class: CoCoClass) -> Result<(), DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<CoCoClass>("classes");
        collection.insert_one(&class).await.map_err(|e| if e.to_string().contains("duplicate key error") { DatabaseError::Exists(class.name.clone()) } else { DatabaseError::ConnectionError(e.to_string()) })?;
        Ok(())
    }

    async fn get_rules(&self) -> Result<Vec<CoCoRule>, DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<CoCoRule>("rules");
        let cursor = collection.find(doc! {}).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        let rules: Vec<CoCoRule> = cursor.try_collect().await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        Ok(rules)
    }

    async fn create_rule(&self, rule: CoCoRule) -> Result<(), DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<CoCoRule>("rules");
        collection.insert_one(&rule).await.map_err(|e| if e.to_string().contains("duplicate key error") { DatabaseError::Exists(rule.name.clone()) } else { DatabaseError::ConnectionError(e.to_string()) })?;
        Ok(())
    }

    async fn get_objects(&self) -> Result<Vec<CoCoObject>, DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<MongoObject>("objects");
        let cursor = collection.find(doc! {}).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        let mongo_objects: Vec<MongoObject> = cursor.try_collect().await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        let objects = mongo_objects.into_iter().map(CoCoObject::from).collect();
        Ok(objects)
    }

    async fn create_object(&self, object: CoCoObject) -> Result<String, DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<MongoObject>("objects");
        let object_id = match object.id.as_deref() {
            Some(id) => Some(ObjectId::parse_str(id).map_err(|e| DatabaseError::InvalidInput(format!("Invalid object id '{id}': {e}")))?),
            None => None,
        };
        let mongo_object = MongoObject {
            id: object_id,
            classes: object.classes.clone(),
            properties: object.properties.as_ref().map(|p| p.iter().map(|(k, v)| (k.clone(), MongoValue::from(v))).collect()),
            values: object.values.as_ref().map(|v| v.iter().map(|(k, tv)| (k.clone(), MongoTimedValue::from(tv))).collect()),
        };
        let result = collection.insert_one(mongo_object).await.map_err(|e| if e.to_string().contains("duplicate key error") { DatabaseError::Exists(object.id.clone().unwrap_or_else(|| "unknown".to_owned())) } else { DatabaseError::ConnectionError(e.to_string()) })?;
        result.inserted_id.as_object_id().map(|id| id.to_hex()).ok_or_else(|| DatabaseError::ConnectionError("Inserted document has non-ObjectId _id".to_owned()))
    }

    async fn add_class(&self, object_id: String, class_name: String) -> Result<(), DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<MongoObject>("objects");
        let oid = ObjectId::parse_str(object_id).map_err(|e| DatabaseError::InvalidInput(format!("Invalid object id: {e}")))?;
        collection.update_one(doc! { "_id": oid }, doc! { "$addToSet": { "classes": class_name } }).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        Ok(())
    }

    async fn set_properties(&self, object_id: String, properties: &HashMap<String, CoCoValue>) -> Result<(), DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<MongoObject>("objects");
        let mut update_doc = doc! {};
        for (prop, value) in properties {
            update_doc.insert(format!("properties.{}", prop), bson::to_bson(&MongoValue::from(value)).map_err(|e| DatabaseError::SerializationError(e.to_string()))?);
        }
        let oid = ObjectId::parse_str(object_id).map_err(|e| DatabaseError::InvalidInput(format!("Invalid object id: {e}")))?;
        collection.update_one(doc! { "_id": oid }, doc! { "$set": update_doc }).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        Ok(())
    }

    async fn add_values(&self, object_id: String, values: HashMap<String, CoCoValue>, date_time: DateTime<Utc>) -> Result<(), DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<MongoObject>("objects");
        let oid = ObjectId::parse_str(object_id.clone()).map_err(|e| DatabaseError::InvalidInput(format!("Invalid object id: {e}")))?;
        let mut update_doc = doc! {};
        let mongo_values: HashMap<String, MongoValue> = values.iter().map(|(k, v)| (k.clone(), MongoValue::from(v))).collect();
        for (prop, mongo_value) in &mongo_values {
            let timed = MongoTimedValue { value: mongo_value.clone(), timestamp: date_time };
            update_doc.insert(format!("values.{}", prop), bson::to_bson(&timed).map_err(|e| DatabaseError::SerializationError(e.to_string()))?);
        }
        collection.update_one(doc! { "_id": oid }, doc! { "$set": update_doc }).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;

        let data_collection = db.collection::<ObjectData>("object_data");
        let data_doc = ObjectData { object_id, values: mongo_values, timestamp: date_time };
        data_collection.insert_one(data_doc).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        Ok(())
    }

    async fn get_values(&self, object_id: String, start_time: Option<DateTime<Utc>>, end_time: Option<DateTime<Utc>>) -> Result<Vec<(HashMap<String, CoCoValue>, DateTime<Utc>)>, DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<ObjectData>("object_data");
        let mut filter = doc! { "object_id": object_id };
        let mut ts_range = doc! {};
        if let Some(start_time) = &start_time {
            ts_range.insert("$gte", bson::to_bson(start_time).map_err(|e| DatabaseError::SerializationError(e.to_string()))?);
        }
        if let Some(end_time) = &end_time {
            ts_range.insert("$lte", bson::to_bson(end_time).map_err(|e| DatabaseError::SerializationError(e.to_string()))?);
        }
        if !ts_range.is_empty() {
            filter.insert("timestamp", ts_range);
        }
        let cursor = collection.find(filter).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        let data: Vec<ObjectData> = cursor.try_collect().await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        let data = data.into_iter().map(|d| (d.values.into_iter().map(|(k, v)| (k, CoCoValue::from(v))).collect(), d.timestamp)).collect();
        Ok(data)
    }

    async fn drop_database(&self) -> Result<(), DatabaseError> {
        self.client.database(&self.name).drop().await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        Ok(())
    }
}

impl From<&CoCoValue> for MongoValue {
    fn from(v: &CoCoValue) -> Self {
        match v {
            CoCoValue::Null => MongoValue::Null,
            CoCoValue::Bool(b) => MongoValue::Bool(*b),
            CoCoValue::Int(i) => MongoValue::Int(*i),
            CoCoValue::Float(f) => MongoValue::Float(*f),
            CoCoValue::String(s) => MongoValue::String(s.clone()),
            CoCoValue::Symbol(s) => MongoValue::Symbol(s.clone()),
            CoCoValue::Object(o) => MongoValue::Object(o.clone()),
            CoCoValue::BoolArray(a) => MongoValue::BoolArray(a.clone()),
            CoCoValue::IntArray(a) => MongoValue::IntArray(a.clone()),
            CoCoValue::FloatArray(a) => MongoValue::FloatArray(a.clone()),
            CoCoValue::StringArray(a) => MongoValue::StringArray(a.clone()),
            CoCoValue::SymbolArray(a) => MongoValue::SymbolArray(a.clone()),
            CoCoValue::ObjectArray(a) => MongoValue::ObjectArray(a.clone()),
        }
    }
}

impl From<MongoValue> for CoCoValue {
    fn from(v: MongoValue) -> Self {
        match v {
            MongoValue::Null => CoCoValue::Null,
            MongoValue::Bool(b) => CoCoValue::Bool(b),
            MongoValue::Int(i) => CoCoValue::Int(i),
            MongoValue::Float(f) => CoCoValue::Float(f),
            MongoValue::String(s) => CoCoValue::String(s),
            MongoValue::Symbol(s) => CoCoValue::Symbol(s),
            MongoValue::Object(o) => CoCoValue::Object(o),
            MongoValue::BoolArray(a) => CoCoValue::BoolArray(a),
            MongoValue::IntArray(a) => CoCoValue::IntArray(a),
            MongoValue::FloatArray(a) => CoCoValue::FloatArray(a),
            MongoValue::StringArray(a) => CoCoValue::StringArray(a),
            MongoValue::SymbolArray(a) => CoCoValue::SymbolArray(a),
            MongoValue::ObjectArray(a) => CoCoValue::ObjectArray(a),
        }
    }
}

impl From<&TimedValue> for MongoTimedValue {
    fn from(tv: &TimedValue) -> Self {
        MongoTimedValue { value: MongoValue::from(&tv.value), timestamp: tv.timestamp }
    }
}

impl From<MongoTimedValue> for TimedValue {
    fn from(tv: MongoTimedValue) -> Self {
        TimedValue { value: CoCoValue::from(tv.value), timestamp: tv.timestamp }
    }
}

impl From<MongoObject> for CoCoObject {
    fn from(o: MongoObject) -> Self {
        CoCoObject {
            id: o.id.map(|oid| oid.to_hex()),
            classes: o.classes,
            properties: o.properties.map(|p| p.into_iter().map(|(k, v)| (k, CoCoValue::from(v))).collect()),
            values: o.values.map(|v| v.into_iter().map(|(k, tv)| (k, TimedValue::from(tv))).collect()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;
    use std::collections::HashSet;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn mongo_uri_from_env() -> String {
        let host = std::env::var("DB_HOST").unwrap_or_else(|_| "localhost".to_owned());
        let port = std::env::var("DB_PORT").unwrap_or_else(|_| "27017".to_owned());
        format!("mongodb://{}:{}", host, port)
    }

    fn unique_db_name(prefix: &str) -> String {
        let unique_suffix = SystemTime::now().duration_since(UNIX_EPOCH).expect("system time should be after UNIX_EPOCH").as_nanos();
        format!("{}_{}", prefix, unique_suffix)
    }

    #[tokio::test]
    async fn connect_and_drop_database() {
        let connection_string = mongo_uri_from_env();
        let db_name = unique_db_name("coco_test_connect");

        let db = MongoDB::new(db_name, connection_string).await.expect("MongoDB connection should succeed");

        Database::drop_database(&db).await.expect("drop_database should succeed");
    }

    #[tokio::test]
    async fn create_class_persists_class() {
        let db = MongoDB::new(unique_db_name("coco_test_class"), mongo_uri_from_env()).await.expect("MongoDB connection should succeed");

        let class = CoCoClass {
            name: "sensor".to_owned(),
            parents: None,
            static_properties: None,
            dynamic_properties: None,
        };

        Database::create_class(&db, class).await.expect("class creation should succeed");

        let classes = Database::get_classes(&db).await.expect("classes retrieval should succeed");
        let stored = classes.into_iter().find(|c| c.name == "sensor");
        assert!(stored.is_some(), "created class should be found in database");

        Database::drop_database(&db).await.expect("drop_database should succeed");
    }

    #[tokio::test]
    async fn create_rule_persists_rule() {
        let db = MongoDB::new(unique_db_name("coco_test_rule"), mongo_uri_from_env()).await.expect("MongoDB connection should succeed");

        let rule = CoCoRule {
            name: "temperature_alert".to_owned(),
            content: "(defrule temperature_alert => (assert (alert)))".to_owned(),
        };

        Database::create_rule(&db, rule).await.expect("rule creation should succeed");

        let rules = Database::get_rules(&db).await.expect("rules retrieval should succeed");
        let stored = rules.into_iter().find(|r| r.name == "temperature_alert");
        assert!(stored.is_some(), "created rule should be found in database");

        Database::drop_database(&db).await.expect("drop_database should succeed");
    }

    #[tokio::test]
    async fn create_object_persists_object() {
        let db = MongoDB::new(unique_db_name("coco_test_object"), mongo_uri_from_env()).await.expect("MongoDB connection should succeed");

        let object = CoCoObject { id: None, classes: HashSet::from(["sensor".to_owned()]), properties: None, values: None };

        let object_id = Database::create_object(&db, object).await.expect("object creation should succeed");

        let objects = Database::get_objects(&db).await.expect("objects retrieval should succeed");
        let stored = objects.into_iter().find(|o| o.id.as_ref() == Some(&object_id));
        assert!(stored.is_some(), "created object should be found in database");

        Database::drop_database(&db).await.expect("drop_database should succeed");
    }

    #[tokio::test]
    async fn get_classes_returns_created_classes() {
        let db = MongoDB::new(unique_db_name("coco_test_get_classes"), mongo_uri_from_env()).await.expect("MongoDB connection should succeed");

        Database::create_class(
            &db,
            CoCoClass {
                name: "sensor".to_owned(),
                parents: None,
                static_properties: None,
                dynamic_properties: None,
            },
        )
        .await
        .expect("first class creation should succeed");

        Database::create_class(
            &db,
            CoCoClass {
                name: "actuator".to_owned(),
                parents: None,
                static_properties: None,
                dynamic_properties: None,
            },
        )
        .await
        .expect("second class creation should succeed");

        let classes = Database::get_classes(&db).await.expect("classes retrieval should succeed");
        let names: HashSet<String> = classes.into_iter().map(|c| c.name).collect();

        assert!(names.contains("sensor"), "retrieved classes should contain sensor");
        assert!(names.contains("actuator"), "retrieved classes should contain actuator");

        Database::drop_database(&db).await.expect("drop_database should succeed");
    }

    #[tokio::test]
    async fn get_rules_returns_created_rules() {
        let db = MongoDB::new(unique_db_name("coco_test_get_rules"), mongo_uri_from_env()).await.expect("MongoDB connection should succeed");

        Database::create_rule(&db, CoCoRule { name: "r1".to_owned(), content: "(defrule r1 => (assert (ok-1)))".to_owned() }).await.expect("first rule creation should succeed");

        Database::create_rule(&db, CoCoRule { name: "r2".to_owned(), content: "(defrule r2 => (assert (ok-2)))".to_owned() }).await.expect("second rule creation should succeed");

        let rules = Database::get_rules(&db).await.expect("rules retrieval should succeed");
        let names: HashSet<String> = rules.into_iter().map(|r| r.name).collect();

        assert!(names.contains("r1"), "retrieved rules should contain r1");
        assert!(names.contains("r2"), "retrieved rules should contain r2");

        Database::drop_database(&db).await.expect("drop_database should succeed");
    }

    #[tokio::test]
    async fn get_objects_returns_created_objects() {
        let db = MongoDB::new(unique_db_name("coco_test_get_objects"), mongo_uri_from_env()).await.expect("MongoDB connection should succeed");

        Database::create_object(&db, CoCoObject { id: None, classes: HashSet::from(["sensor".to_owned()]), properties: None, values: None }).await.expect("first object creation should succeed");

        Database::create_object(&db, CoCoObject { id: None, classes: HashSet::from(["actuator".to_owned()]), properties: None, values: None }).await.expect("second object creation should succeed");

        let objects = Database::get_objects(&db).await.expect("objects retrieval should succeed");

        assert!(objects.len() >= 2, "retrieved objects should include the two created objects");
        assert!(objects.iter().any(|o| o.classes.contains("sensor")), "one retrieved object should contain class sensor");
        assert!(objects.iter().any(|o| o.classes.contains("actuator")), "one retrieved object should contain class actuator");

        Database::drop_database(&db).await.expect("drop_database should succeed");
    }
}
