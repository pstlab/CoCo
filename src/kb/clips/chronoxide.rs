use crate::{
    CoCo, CoCoModule,
    db::Database,
    kb::KnowledgeBase,
    kb::clips::CLIPSKnowledgeBase,
    model::{CoCoClass, CoCoError, CoCoObject, CoCoProperty, CoCoValue},
};
use async_trait::async_trait;
use clips::{ClipsValue, Type, UDFContext};
use std::collections::{HashMap, HashSet};
use tracing::{error, info, trace};

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
    async fn init(&self, _db: DB, kb: CLIPSKnowledgeBase, coco: CoCo) -> Result<(), CoCoError> {
        if kb.get_class("Class").await.map_err(|_| CoCoError::DatabaseError("Failed to retrieve 'Class' class from database".to_string()))?.is_none() {
            coco.create_class(CoCoClass {
                name: "Class".to_string(),
                static_properties: Some(HashMap::from([
                    (
                        "name".to_string(),
                        CoCoProperty::Symbol {
                            default: None,
                            allowed_values: None,
                            description: Some("The name of the class".to_string()),
                        },
                    ),
                    ("content".to_string(), CoCoProperty::String { default: None, description: Some("The content of the class, in RiDDLe format".to_string()) }),
                ])),
                dynamic_properties: None,
                parents: None,
            })
            .await?;

            info!("Successfully created 'Class' class in database");
        }
        if kb.get_class("Predicate").await.map_err(|_| CoCoError::DatabaseError("Failed to retrieve 'Predicate' class from database".to_string()))?.is_none() {
            coco.create_class(CoCoClass {
                name: "Predicate".to_string(),
                static_properties: Some(HashMap::from([
                    (
                        "name".to_string(),
                        CoCoProperty::Symbol {
                            default: None,
                            allowed_values: None,
                            description: Some("The name of the predicate".to_string()),
                        },
                    ),
                    (
                        "class".to_string(),
                        CoCoProperty::Object {
                            default: None,
                            classes: vec!["Class".to_string()],
                            description: Some("The class this predicate belongs to".to_string()),
                        },
                    ),
                    ("parameters".to_string(), CoCoProperty::StringArray { default: None, description: Some("The parameters of the predicate".to_string()) }),
                    (
                        "content".to_string(),
                        CoCoProperty::String {
                            default: None,
                            description: Some("The content of the predicate, in RiDDLe format".to_string()),
                        },
                    ),
                ])),
                dynamic_properties: None,
                parents: None,
            })
            .await?;

            info!("Successfully created 'Predicate' class in database");
        }
        if kb.get_class("Impulse").await.map_err(|_| CoCoError::DatabaseError("Failed to retrieve 'Impulse' class from database".to_string()))?.is_none() {
            coco.create_class(CoCoClass {
                name: "Impulse".to_string(),
                static_properties: Some(HashMap::from([(
                    "at".to_string(),
                    CoCoProperty::Float {
                        default: None,
                        min: Some(0.0),
                        max: None,
                        description: Some("The time at which the impulse occurs".to_string()),
                    },
                )])),
                dynamic_properties: None,
                parents: Some(HashSet::from(["Predicate".to_string()])),
            })
            .await?;

            info!("Successfully created 'Impulse' class in database");
        }
        if kb.get_class("Interval").await.map_err(|_| CoCoError::DatabaseError("Failed to retrieve 'Interval' class from database".to_string()))?.is_none() {
            coco.create_class(CoCoClass {
                name: "Interval".to_string(),
                static_properties: Some(HashMap::from([
                    (
                        "start".to_string(),
                        CoCoProperty::Float {
                            default: None,
                            min: Some(0.0),
                            max: None,
                            description: Some("The start time of the interval".to_string()),
                        },
                    ),
                    (
                        "end".to_string(),
                        CoCoProperty::Float {
                            default: None,
                            min: Some(0.0),
                            max: None,
                            description: Some("The end time of the interval".to_string()),
                        },
                    ),
                ])),
                dynamic_properties: None,
                parents: Some(HashSet::from(["Predicate".to_string()])),
            })
            .await?;

            info!("Successfully created 'Interval' class in database");
        }

        if kb.get_class("StateVariable").await.map_err(|_| CoCoError::DatabaseError("Failed to retrieve 'StateVariable' class from database".to_string()))?.is_none() {
            coco.create_class(CoCoClass {
                name: "StateVariable".to_string(),
                static_properties: None,
                dynamic_properties: None,
                parents: Some(HashSet::from(["Class".to_string()])),
            })
            .await?;

            info!("Successfully created 'StateVariable' class in database");
        }
        if kb.get_class("ReusableResource").await.map_err(|_| CoCoError::DatabaseError("Failed to retrieve 'ReusableResource' class from database".to_string()))?.is_none() {
            coco.create_class(CoCoClass {
                name: "ReusableResource".to_string(),
                static_properties: Some(HashMap::from([(
                    "capacity".to_string(),
                    CoCoProperty::Float {
                        default: None,
                        min: Some(0.0),
                        max: None,
                        description: Some("The maximum capacity of the resource".to_string()),
                    },
                )])),
                dynamic_properties: None,
                parents: Some(HashSet::from(["Class".to_string()])),
            })
            .await?;
            coco.create_object(CoCoObject {
                id: None,
                classes: HashSet::from(["Interval".to_string()]),
                properties: Some(HashMap::from([("name".to_string(), CoCoValue::Symbol("Use".to_string())), ("parameters".to_string(), CoCoValue::StringArray(vec!["real amount".to_string()])), ("content".to_string(), CoCoValue::String("".to_string()))])),
                values: None,
            })
            .await?;

            info!("Successfully created 'ReusableResource' class in database");
        }
        if kb.get_class("ConsumableResource").await.map_err(|_| CoCoError::DatabaseError("Failed to retrieve 'ConsumableResource' class from database".to_string()))?.is_none() {
            coco.create_class(CoCoClass {
                name: "ConsumableResource".to_string(),
                static_properties: Some(HashMap::from([
                    (
                        "capacity".to_string(),
                        CoCoProperty::Float {
                            default: None,
                            min: Some(0.0),
                            max: None,
                            description: Some("The maximum capacity of the resource".to_string()),
                        },
                    ),
                    (
                        "initial_amount".to_string(),
                        CoCoProperty::Float {
                            default: Some(0.0),
                            min: Some(0.0),
                            max: None,
                            description: Some("The initial amount of the resource".to_string()),
                        },
                    ),
                ])),
                dynamic_properties: None,
                parents: Some(HashSet::from(["Class".to_string()])),
            })
            .await?;
            coco.create_object(CoCoObject {
                id: None,
                classes: HashSet::from(["Interval".to_string()]),
                properties: Some(HashMap::from([("name".to_string(), CoCoValue::Symbol("Produce".to_string())), ("parameters".to_string(), CoCoValue::StringArray(vec!["real amount".to_string()])), ("content".to_string(), CoCoValue::String("".to_string()))])),
                values: None,
            })
            .await?;
            coco.create_object(CoCoObject {
                id: None,
                classes: HashSet::from(["Interval".to_string()]),
                properties: Some(HashMap::from([("name".to_string(), CoCoValue::Symbol("Consume".to_string())), ("parameters".to_string(), CoCoValue::StringArray(vec!["real amount".to_string()])), ("content".to_string(), CoCoValue::String("".to_string()))])),
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
                let solver_id = match ctx.get_next_argument(Type(Type::SYMBOL)) {
                    Some(ClipsValue::Symbol(s)) => s.to_string(),
                    _ => {
                        error!("Expected symbol for solver ID argument in create-solver UDF");
                        return ClipsValue::Void();
                    }
                };

                let riddle = match ctx.get_next_argument(Type(Type::STRING)) {
                    Some(ClipsValue::String(s)) => s.to_string(),
                    _ => {
                        error!("Expected string for RiDDLe content argument in create-solver UDF");
                        return ClipsValue::Void();
                    }
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
