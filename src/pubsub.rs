use crate::{
    CoCo, CoCoModule,
    db::Database,
    kb::KnowledgeBase,
    model::{CoCoError, properties_from_json, values_from_json},
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use google_cloud_pubsub::client::Subscriber;
use serde_json::Value;
use std::env;
use tracing::{error, info, warn};

pub struct PubSubModule {
    subscription_name: String,
}

impl PubSubModule {
    pub fn new(subscription_name: String) -> Self {
        Self { subscription_name }
    }
}

impl Default for PubSubModule {
    fn default() -> Self {
        let subscription_name = env::var("PUBSUB_SUBSCRIPTION_NAME").expect("PUBSUB_SUBSCRIPTION_NAME environment variable is not set");
        Self::new(subscription_name)
    }
}

#[async_trait]
impl<DB: Database, KB: KnowledgeBase> CoCoModule<DB, KB> for PubSubModule {
    async fn init(&self, _db: DB, _kb: KB, coco: CoCo) -> Result<(), CoCoError> {
        info!("Starting Pub/Sub client with subscription: {}", self.subscription_name);

        let client = Subscriber::builder().build().await.map_err(|e| CoCoError::ConfigurationError(format!("Failed to create Pub/Sub subscriber: {}", e)))?;

        let subscription_name = self.subscription_name.clone();

        tokio::spawn(async move {
            let mut stream = client.subscribe(subscription_name).build();

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
                                hndl.ack();
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
