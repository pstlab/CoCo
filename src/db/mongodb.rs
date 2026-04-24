use std::collections::{HashMap, HashSet};

use crate::db::{Database, DatabaseError};
use crate::model::{Class, Object, Rule, TimedValue, Value};
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
    pub properties: Option<HashMap<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<HashMap<String, TimedValue>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct ObjectData {
    pub object_id: String,
    pub values: HashMap<String, Value>,
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

    async fn get_classes(&self) -> Result<Vec<Class>, DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<Class>("classes");
        let cursor = collection.find(doc! {}).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        let classes: Vec<Class> = cursor.try_collect().await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        Ok(classes)
    }

    async fn get_class(&self, name: &str) -> Result<Option<Class>, DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<Class>("classes");
        let class = collection.find_one(doc! { "name": name }).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        Ok(class)
    }

    async fn create_class(&self, class: Class) -> Result<(), DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<Class>("classes");
        collection.insert_one(&class).await.map_err(|e| if e.to_string().contains("duplicate key error") { DatabaseError::Exists(class.name.clone()) } else { DatabaseError::ConnectionError(e.to_string()) })?;
        Ok(())
    }

    async fn get_rules(&self) -> Result<Vec<Rule>, DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<Rule>("rules");
        let cursor = collection.find(doc! {}).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        let rules: Vec<Rule> = cursor.try_collect().await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        Ok(rules)
    }

    async fn get_rule(&self, name: &str) -> Result<Option<Rule>, DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<Rule>("rules");
        let rule = collection.find_one(doc! { "name": name }).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        Ok(rule)
    }

    async fn create_rule(&self, rule: Rule) -> Result<(), DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<Rule>("rules");
        collection.insert_one(&rule).await.map_err(|e| if e.to_string().contains("duplicate key error") { DatabaseError::Exists(rule.name.clone()) } else { DatabaseError::ConnectionError(e.to_string()) })?;
        Ok(())
    }

    async fn get_objects(&self) -> Result<Vec<Object>, DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<MongoObject>("objects");
        let cursor = collection.find(doc! {}).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        let mongo_objects: Vec<MongoObject> = cursor.try_collect().await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        let objects = mongo_objects
            .into_iter()
            .map(|mongo_object| Object {
                id: mongo_object.id.map(|oid| oid.to_hex()),
                classes: mongo_object.classes,
                properties: mongo_object.properties,
                values: mongo_object.values,
            })
            .collect();
        Ok(objects)
    }

    async fn get_object(&self, object_id: String) -> Result<Option<Object>, DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<MongoObject>("objects");
        let oid = ObjectId::parse_str(object_id).map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        let mongo_object = collection.find_one(doc! { "_id": oid }).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        if let Some(mongo_object) = mongo_object {
            Ok(Some(Object {
                id: mongo_object.id.map(|oid| oid.to_hex()),
                classes: mongo_object.classes,
                properties: mongo_object.properties,
                values: mongo_object.values,
            }))
        } else {
            Ok(None)
        }
    }

    async fn create_object(&self, object: Object) -> Result<String, DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<MongoObject>("objects");
        let mongo_object = MongoObject {
            id: None,
            classes: object.classes.clone(),
            properties: object.properties.clone(),
            values: object.values.clone(),
        };
        let result = collection.insert_one(mongo_object).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        Ok(result.inserted_id.as_object_id().unwrap().to_hex())
    }

    async fn add_class(&self, object_id: String, class_name: String) -> Result<(), DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<MongoObject>("objects");
        let oid = ObjectId::parse_str(object_id).map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        collection.update_one(doc! { "_id": oid }, doc! { "$addToSet": { "classes": class_name } }).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        Ok(())
    }

    async fn set_properties(&self, object_id: String, properties: &HashMap<String, Value>) -> Result<(), DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<MongoObject>("objects");
        let mut update_doc = doc! {};
        for (prop, value) in properties {
            update_doc.insert(format!("properties.{}", prop), bson::to_bson(value).map_err(|e| DatabaseError::ConnectionError(e.to_string()))?);
        }
        let oid = ObjectId::parse_str(object_id).map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        collection.update_one(doc! { "_id": oid }, doc! { "$set": update_doc }).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        Ok(())
    }

    async fn add_values(&self, object_id: String, values: HashMap<String, Value>, date_time: DateTime<Utc>) -> Result<(), DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<MongoObject>("objects");
        let oid = ObjectId::parse_str(object_id.clone()).map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        let mut update_doc = doc! {};
        for (prop, value) in &values {
            update_doc.insert(format!("values.{}", prop), bson::to_bson(&(value.clone(), date_time)).map_err(|e| DatabaseError::ConnectionError(e.to_string()))?);
        }
        collection.update_one(doc! { "_id": oid }, doc! { "$set": update_doc }).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;

        let data_collection = db.collection::<ObjectData>("object_data");
        let data_doc = ObjectData { object_id, values, timestamp: date_time };
        data_collection.insert_one(data_doc).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        Ok(())
    }

    async fn get_values(&self, object_id: String, start_time: Option<DateTime<Utc>>, end_time: Option<DateTime<Utc>>) -> Result<Vec<(HashMap<String, Value>, DateTime<Utc>)>, DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<ObjectData>("object_data");
        let mut filter = doc! { "object_id": object_id };
        let mut ts_range = doc! {};
        if let Some(start_time) = &start_time {
            ts_range.insert("$gte", bson::to_bson(start_time).unwrap());
        }
        if let Some(end_time) = &end_time {
            ts_range.insert("$lte", bson::to_bson(end_time).unwrap());
        }
        if !ts_range.is_empty() {
            filter.insert("timestamp", ts_range);
        }
        let cursor = collection.find(filter).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        let data: Vec<ObjectData> = cursor.try_collect().await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        let data = data.into_iter().map(|d| (d.values, d.timestamp)).collect();
        Ok(data)
    }

    async fn drop_database(&self) -> Result<(), DatabaseError> {
        self.client.database(&self.name).drop().await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        Ok(())
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

        let class = Class {
            name: "sensor".to_owned(),
            parents: None,
            static_properties: None,
            dynamic_properties: None,
        };

        Database::create_class(&db, class).await.expect("class creation should succeed");

        let stored = Database::get_class(&db, "sensor").await.expect("class retrieval should succeed");
        assert!(stored.is_some(), "created class should be found in database");

        Database::drop_database(&db).await.expect("drop_database should succeed");
    }

    #[tokio::test]
    async fn create_rule_persists_rule() {
        let db = MongoDB::new(unique_db_name("coco_test_rule"), mongo_uri_from_env()).await.expect("MongoDB connection should succeed");

        let rule = Rule {
            name: "temperature_alert".to_owned(),
            content: "(defrule temperature_alert => (assert (alert)))".to_owned(),
        };

        Database::create_rule(&db, rule).await.expect("rule creation should succeed");

        let stored = Database::get_rule(&db, "temperature_alert").await.expect("rule retrieval should succeed");
        assert!(stored.is_some(), "created rule should be found in database");

        Database::drop_database(&db).await.expect("drop_database should succeed");
    }

    #[tokio::test]
    async fn create_object_persists_object() {
        let db = MongoDB::new(unique_db_name("coco_test_object"), mongo_uri_from_env()).await.expect("MongoDB connection should succeed");

        let object = Object { id: None, classes: HashSet::from(["sensor".to_owned()]), properties: None, values: None };

        let object_id = Database::create_object(&db, object).await.expect("object creation should succeed");

        let stored = Database::get_object(&db, object_id).await.expect("object retrieval should succeed");
        assert!(stored.is_some(), "created object should be found in database");

        Database::drop_database(&db).await.expect("drop_database should succeed");
    }

    #[tokio::test]
    async fn get_classes_returns_created_classes() {
        let db = MongoDB::new(unique_db_name("coco_test_get_classes"), mongo_uri_from_env()).await.expect("MongoDB connection should succeed");

        Database::create_class(
            &db,
            Class {
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
            Class {
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

        Database::create_rule(&db, Rule { name: "r1".to_owned(), content: "(defrule r1 => (assert (ok-1)))".to_owned() }).await.expect("first rule creation should succeed");

        Database::create_rule(&db, Rule { name: "r2".to_owned(), content: "(defrule r2 => (assert (ok-2)))".to_owned() }).await.expect("second rule creation should succeed");

        let rules = Database::get_rules(&db).await.expect("rules retrieval should succeed");
        let names: HashSet<String> = rules.into_iter().map(|r| r.name).collect();

        assert!(names.contains("r1"), "retrieved rules should contain r1");
        assert!(names.contains("r2"), "retrieved rules should contain r2");

        Database::drop_database(&db).await.expect("drop_database should succeed");
    }

    #[tokio::test]
    async fn get_objects_returns_created_objects() {
        let db = MongoDB::new(unique_db_name("coco_test_get_objects"), mongo_uri_from_env()).await.expect("MongoDB connection should succeed");

        Database::create_object(&db, Object { id: None, classes: HashSet::from(["sensor".to_owned()]), properties: None, values: None }).await.expect("first object creation should succeed");

        Database::create_object(&db, Object { id: None, classes: HashSet::from(["actuator".to_owned()]), properties: None, values: None }).await.expect("second object creation should succeed");

        let objects = Database::get_objects(&db).await.expect("objects retrieval should succeed");

        assert!(objects.len() >= 2, "retrieved objects should include the two created objects");
        assert!(objects.iter().any(|o| o.classes.contains("sensor")), "one retrieved object should contain class sensor");
        assert!(objects.iter().any(|o| o.classes.contains("actuator")), "one retrieved object should contain class actuator");

        Database::drop_database(&db).await.expect("drop_database should succeed");
    }
}
