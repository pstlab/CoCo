use crate::{
    CoCo, CoCoModule,
    db::Database,
    kb::KnowledgeBase,
    model::{CoCoError, CoCoEvent, properties_from_json, values_from_json},
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use google_cloud_pubsub::{
    client::{Publisher, Subscriber},
    model::Message,
};
use serde_json::Value;
use std::{collections::HashMap, env};
use tracing::{error, info, warn};

pub struct PubSubModule {
    subscription_name: String,
    publish_topic: String,
}

impl PubSubModule {
    pub fn new(subscription_name: String, publish_topic: String) -> Self {
        Self { subscription_name, publish_topic }
    }
}

impl Default for PubSubModule {
    fn default() -> Self {
        let subscription_name = env::var("PUBSUB_SUBSCRIPTION_NAME").expect("PUBSUB_SUBSCRIPTION_NAME environment variable is not set");
        let publish_topic = env::var("PUBSUB_PUBLISH_TOPIC").unwrap_or_else(|_| "coco-events".to_string());
        Self::new(subscription_name, publish_topic)
    }
}

#[async_trait]
impl<DB: Database, KB: KnowledgeBase> CoCoModule<DB, KB> for PubSubModule {
    async fn init(&self, _db: DB, _kb: KB, coco: CoCo) -> Result<(), CoCoError> {
        info!("Starting Pub/Sub client with subscription: {}", self.subscription_name);

        let publisher = Publisher::builder(&self.publish_topic).build().await.map_err(|e| CoCoError::ConfigurationError(format!("Failed to create Pub/Sub publisher: {}", e)))?;

        let mut rx = coco.event_tx.subscribe();
        let coco_clone = coco.clone();

        tokio::spawn(async move {
            while let Ok(msg) = rx.recv().await {
                match msg {
                    CoCoEvent::ClassCreated(class_name) => {
                        if let Ok(class) = coco_clone.get_class(class_name.clone()).await {
                            if let Ok(data) = serde_json::to_string(&class) {
                                let publisher_task = publisher.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = publisher_task.publish(Message::new().set_attributes(HashMap::from([("msg_type".to_string(), "class-created".to_string()), ("class_name".to_string(), class_name.clone())])).set_data(data.into_bytes())).await {
                                        error!("Failed to publish class-created event for class '{}': {}", class_name, e);
                                    }
                                });
                            } else {
                                error!("Failed to serialize class data for class '{}'", class_name);
                            }
                        } else {
                            error!("Failed to retrieve class data for class '{}'", class_name);
                        }
                    }
                    CoCoEvent::ObjectCreated(object_id) => {
                        if let Ok(object) = coco_clone.get_object(object_id.clone()).await {
                            if let Ok(data) = serde_json::to_string(&object) {
                                let publisher_task = publisher.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = publisher_task.publish(Message::new().set_attributes(HashMap::from([("msg_type".to_string(), "object-created".to_string()), ("object_id".to_string(), object_id.clone())])).set_data(data.into_bytes())).await {
                                        error!("Failed to publish object-created event for object '{}': {}", object_id, e);
                                    }
                                });
                            } else {
                                error!("Failed to serialize object data for object '{}'", object_id);
                            }
                        } else {
                            error!("Failed to retrieve object data for object '{}'", object_id);
                        }
                    }
                    CoCoEvent::ClassesUpdated(object_id, classes) => {
                        if let Ok(data) = serde_json::to_string(&serde_json::json!({
                            "object_id": object_id,
                            "classes": classes
                        })) {
                            let publisher_task = publisher.clone();
                            tokio::spawn(async move {
                                if let Err(e) = publisher_task.publish(Message::new().set_attributes(HashMap::from([("msg_type".to_string(), "classes-updated".to_string()), ("object_id".to_string(), object_id.clone())])).set_data(data.into_bytes())).await {
                                    error!("Failed to publish classes-updated event for object '{}': {}", object_id, e);
                                }
                            });
                        } else {
                            error!("Failed to serialize classes data for object '{}'", object_id);
                        }
                    }
                    CoCoEvent::PropertiesUpdated(object_id, properties) => {
                        if let Ok(data) = serde_json::to_string(&properties) {
                            let publisher_task = publisher.clone();
                            tokio::spawn(async move {
                                if let Err(e) = publisher_task.publish(Message::new().set_attributes(HashMap::from([("msg_type".to_string(), "properties-updated".to_string()), ("object_id".to_string(), object_id.clone())])).set_data(data.into_bytes())).await {
                                    error!("Failed to publish properties-updated event for object '{}': {}", object_id, e);
                                }
                            });
                        } else {
                            error!("Failed to serialize properties data for object '{}'", object_id);
                        }
                    }
                    CoCoEvent::ValuesAdded(object_id, values, timestamp) => {
                        if let Ok(data) = serde_json::to_string(&values) {
                            let publisher_task = publisher.clone();
                            tokio::spawn(async move {
                                if let Err(e) = publisher_task.publish(Message::new().set_attributes(HashMap::from([("msg_type".to_string(), "values-added".to_string()), ("object_id".to_string(), object_id.clone()), ("timestamp".to_string(), timestamp.timestamp().to_string())])).set_data(data.into_bytes())).await {
                                    error!("Failed to publish values-added event for object '{}': {}", object_id, e);
                                }
                            });
                        } else {
                            error!("Failed to serialize values data for object '{}'", object_id);
                        }
                    }
                    CoCoEvent::RuleCreated(rule) => {
                        if let Ok(rule_data) = coco_clone.get_rule(rule.clone()).await {
                            if let Ok(data) = serde_json::to_string(&rule_data) {
                                let publisher_task = publisher.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = publisher_task.publish(Message::new().set_attributes(HashMap::from([("msg_type".to_string(), "rule-created".to_string()), ("rule_name".to_string(), rule.clone())])).set_data(data.into_bytes())).await {
                                        error!("Failed to publish rule-created event for rule '{}': {}", rule, e);
                                    }
                                });
                            } else {
                                error!("Failed to serialize rule data for rule '{}'", rule);
                            }
                        } else {
                            error!("Failed to retrieve rule data for rule '{}'", rule);
                        }
                    }
                };
            }
        });

        let subscriber = Subscriber::builder().build().await.map_err(|e| CoCoError::ConfigurationError(format!("Failed to create Pub/Sub subscriber: {}", e)))?;

        let subscription_name = self.subscription_name.clone();

        tokio::spawn(async move {
            let mut stream = subscriber.subscribe(subscription_name).build();

            while let Some(msg) = stream.next().await {
                match msg {
                    Ok((msg, hndl)) => {
                        let Some(msg_type) = msg.attributes.get("msg_type") else {
                            warn!("Received Pub/Sub message without msg_type attribute");
                            hndl.ack();
                            continue;
                        };

                        let Some(object_id) = msg.attributes.get("object_id") else {
                            warn!("Received Pub/Sub message without object_id attribute");
                            hndl.ack();
                            continue;
                        };

                        let values = match serde_json::from_slice::<Value>(&msg.data) {
                            Ok(v) => v,
                            Err(e) => {
                                warn!("Failed to parse Pub/Sub message data as JSON: {}", e);
                                hndl.ack();
                                continue;
                            }
                        };

                        let classes = match coco.get_object_classes(object_id.clone()).await {
                            Ok(c) => c,
                            Err(CoCoError::ObjectNotFound(e)) => {
                                warn!("Object not found for ID '{}': {}", object_id, e);
                                hndl.ack();
                                continue;
                            }
                            Err(e) => {
                                warn!("Failed to get classes for object with ID '{}': {}", object_id, e);
                                hndl.nack();
                                continue;
                            }
                        };

                        match msg_type.as_str() {
                            "static" => match properties_from_json(coco.clone(), classes, values).await {
                                Ok(properties) => {
                                    if let Err(e) = coco.set_properties(object_id.clone(), properties).await {
                                        error!("Failed to set properties for object with ID '{}': {}", object_id, e);
                                    }
                                }
                                Err(e) => error!("Failed to parse properties from Pub/Sub message for object with ID '{}': {}", object_id, e),
                            },
                            "dynamic" => {
                                let timestamp = msg.attributes.get("timestamp").and_then(|ts| ts.parse::<i64>().ok()).and_then(|secs| DateTime::from_timestamp(secs, 0)).unwrap_or_else(Utc::now);

                                match values_from_json(coco.clone(), classes, values).await {
                                    Ok(dynamic_values) => {
                                        if let Err(e) = coco.add_values(object_id.clone(), dynamic_values, timestamp).await {
                                            error!("Failed to add values for object with ID '{}': {}", object_id, e);
                                        }
                                    }
                                    Err(e) => error!("Failed to parse values from Pub/Sub message for object with ID '{}': {}", object_id, e),
                                }
                            }
                            _ => warn!("Received Pub/Sub message with unknown msg_type: {}", msg_type),
                        }

                        hndl.ack();
                    }
                    Err(e) => {
                        error!("Error receiving message from Pub/Sub: {}", e);
                    }
                }
            }
        });

        Ok(())
    }
}
