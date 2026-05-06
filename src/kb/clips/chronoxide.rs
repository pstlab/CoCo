use std::collections::{HashMap, HashSet};

use crate::{
    CoCo, CoCoModule,
    db::Database,
    kb::clips::CLIPSKnowledgeBase,
    model::{Class, CoCoError, Object, Property, Value},
};
use async_trait::async_trait;
use clips::{ClipsValue, Type, UDFContext};
use tracing::{info, trace};

pub struct ChronoxideModule {}

impl ChronoxideModule {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for ChronoxideModule {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<DB: Database> CoCoModule<DB, CLIPSKnowledgeBase> for ChronoxideModule {
    async fn init(&self, db: DB, kb: CLIPSKnowledgeBase, coco: CoCo) -> Result<(), CoCoError> {
        if db.get_class("Class").await.map_err(|_| CoCoError::DatabaseError("Failed to retrieve 'Class' class from database".to_string()))?.is_none() {
            coco.create_class(Class {
                name: "Class".to_string(),
                static_properties: Some(HashMap::from([("name".to_string(), Property::Symbol { default: None, allowed_values: None }), ("content".to_string(), Property::String { default: None })])),
                dynamic_properties: None,
                parents: None,
            })
            .await?;

            info!("Successfully created 'Class' class in database");
        }
        if db.get_class("Predicate").await.map_err(|_| CoCoError::DatabaseError("Failed to retrieve 'Predicate' class from database".to_string()))?.is_none() {
            coco.create_class(Class {
                name: "Predicate".to_string(),
                static_properties: Some(HashMap::from([
                    ("name".to_string(), Property::Symbol { default: None, allowed_values: None }),
                    ("class".to_string(), Property::Object { default: None, class: "Class".to_string() }),
                    ("parameters".to_string(), Property::StringArray { default: None }),
                    ("content".to_string(), Property::String { default: None }),
                ])),
                dynamic_properties: None,
                parents: None,
            })
            .await?;

            info!("Successfully created 'Predicate' class in database");
        }
        if db.get_class("Impulse").await.map_err(|_| CoCoError::DatabaseError("Failed to retrieve 'Impulse' class from database".to_string()))?.is_none() {
            coco.create_class(Class {
                name: "Impulse".to_string(),
                static_properties: Some(HashMap::from([("at".to_string(), Property::Float { default: None, min: Some(0.0), max: None })])),
                dynamic_properties: None,
                parents: Some(HashSet::from(["Predicate".to_string()])),
            })
            .await?;

            info!("Successfully created 'Impulse' class in database");
        }
        if db.get_class("Interval").await.map_err(|_| CoCoError::DatabaseError("Failed to retrieve 'Interval' class from database".to_string()))?.is_none() {
            coco.create_class(Class {
                name: "Interval".to_string(),
                static_properties: Some(HashMap::from([("start".to_string(), Property::Float { default: None, min: Some(0.0), max: None }), ("end".to_string(), Property::Float { default: None, min: Some(0.0), max: None })])),
                dynamic_properties: None,
                parents: Some(HashSet::from(["Predicate".to_string()])),
            })
            .await?;

            info!("Successfully created 'Interval' class in database");
        }

        if db.get_class("StateVariable").await.map_err(|_| CoCoError::DatabaseError("Failed to retrieve 'StateVariable' class from database".to_string()))?.is_none() {
            coco.create_class(Class {
                name: "StateVariable".to_string(),
                static_properties: None,
                dynamic_properties: None,
                parents: Some(HashSet::from(["Class".to_string()])),
            })
            .await?;

            info!("Successfully created 'StateVariable' class in database");
        }
        if db.get_class("ReusableResource").await.map_err(|_| CoCoError::DatabaseError("Failed to retrieve 'ReusableResource' class from database".to_string()))?.is_none() {
            coco.create_class(Class {
                name: "ReusableResource".to_string(),
                static_properties: Some(HashMap::from([("capacity".to_string(), Property::Float { default: None, min: Some(0.0), max: None })])),
                dynamic_properties: None,
                parents: Some(HashSet::from(["Class".to_string()])),
            })
            .await?;
            coco.create_object(Object {
                id: None,
                classes: HashSet::from(["Interval".to_string()]),
                properties: Some(HashMap::from([("name".to_string(), Value::Symbol("Use".to_string())), ("parameters".to_string(), Value::StringArray(vec!["real amount".to_string()])), ("content".to_string(), Value::String("".to_string()))])),
                values: None,
            })
            .await?;

            info!("Successfully created 'ReusableResource' class in database");
        }
        if db.get_class("ConsumableResource").await.map_err(|_| CoCoError::DatabaseError("Failed to retrieve 'ConsumableResource' class from database".to_string()))?.is_none() {
            coco.create_class(Class {
                name: "ConsumableResource".to_string(),
                static_properties: Some(HashMap::from([("capacity".to_string(), Property::Float { default: None, min: Some(0.0), max: None }), ("initial_amount".to_string(), Property::Float { default: Some(0.0), min: Some(0.0), max: None })])),
                dynamic_properties: None,
                parents: Some(HashSet::from(["Class".to_string()])),
            })
            .await?;
            coco.create_object(Object {
                id: None,
                classes: HashSet::from(["Interval".to_string()]),
                properties: Some(HashMap::from([("name".to_string(), Value::Symbol("Produce".to_string())), ("parameters".to_string(), Value::StringArray(vec!["real amount".to_string()])), ("content".to_string(), Value::String("".to_string()))])),
                values: None,
            })
            .await?;
            coco.create_object(Object {
                id: None,
                classes: HashSet::from(["Interval".to_string()]),
                properties: Some(HashMap::from([("name".to_string(), Value::Symbol("Consume".to_string())), ("parameters".to_string(), Value::StringArray(vec!["real amount".to_string()])), ("content".to_string(), Value::String("".to_string()))])),
                values: None,
            })
            .await?;

            info!("Successfully created 'ConsumableResource' class in database");
        }

        kb.add_udf(
            "create-solver",
            None,
            2,
            2,
            vec![Type(Type::SYMBOL), Type(Type::STRING)],
            Box::new(move |_env, ctx: &mut UDFContext| {
                let solver_id = match ctx.get_next_argument(Type(Type::SYMBOL)).expect("Failed to get solver ID argument for create-solver UDF") {
                    ClipsValue::Symbol(s) => s.to_string(),
                    _ => panic!("Expected symbol for solver ID argument in create-solver UDF"),
                };

                let riddle = match ctx.get_next_argument(Type(Type::STRING)).expect("Failed to get RiDDLe content argument for create-solver UDF") {
                    ClipsValue::String(s) => s.to_string(),
                    _ => panic!("Expected string for RiDDLe content argument in create-solver UDF"),
                };

                trace!("Creating Chronoxide solver with ID {} and RiDDLe content: {}", solver_id, riddle);
                ClipsValue::Void()
            }),
        )
        .await
        .map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to add prompt UDF: {}", e)))?;

        Ok(())
    }
}
