use crate::{
    CoCo, CoCoModule,
    db::Database,
    kb::KnowledgeBase,
    model::{CoCoError, CoCoEvent, properties_from_json, values_from_json},
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rumqttc::v5::{
    AsyncClient, Event, MqttOptions,
    mqttbytes::{
        QoS,
        v5::{Filter, Packet},
    },
};
use serde_json::Value;
use std::{env, time::Duration};
use tracing::{error, info, trace, warn};

pub struct MQTTModule {
    broker: String,
    port: u16,
}

impl MQTTModule {
    pub fn new(broker: String, port: u16) -> Self {
        Self { broker, port }
    }
}

impl Default for MQTTModule {
    fn default() -> Self {
        let broker = env::var("MQTT_BROKER").unwrap_or_else(|_| "localhost".to_string());
        let port = env::var("MQTT_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(1883);
        Self::new(broker, port)
    }
}

#[async_trait]
impl<DB: Database, KB: KnowledgeBase> CoCoModule<DB, KB> for MQTTModule {
    async fn init(&self, _db: DB, _kb: KB, coco: CoCo) -> Result<(), CoCoError> {
        info!("Starting MQTT client connecting to {}:{}", self.broker, self.port);
        let mut mqtt_options = MqttOptions::new("coco-client-id", self.broker.as_str(), self.port);
        mqtt_options.set_keep_alive(Duration::from_secs(5));
        let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);

        let mut rx = coco.event_tx.subscribe();
        let coco_clone = coco.clone();
        tokio::spawn(async move {
            while let Ok(msg) = rx.recv().await {
                match msg {
                    CoCoEvent::ClassCreated(class_name) => {
                        if let Ok(class) = coco_clone.get_class(class_name.clone()).await {
                            if let Ok(mut update_msg) = serde_json::to_value(class) {
                                update_msg["msg_type"] = serde_json::json!("class-created");
                                if let Ok(payload) = serde_json::to_string(&update_msg) {
                                    if let Err(e) = client.publish("coco/events", QoS::AtLeastOnce, false, payload).await {
                                        error!("Failed to publish class-created event for class '{}': {}", class_name, e);
                                    }
                                }
                            }
                        }
                    }
                    CoCoEvent::ObjectCreated(object_id) => {
                        if let Ok(object) = coco_clone.get_object(object_id.clone()).await {
                            if let Ok(mut update_msg) = serde_json::to_value(object) {
                                update_msg["msg_type"] = serde_json::json!("object-created");
                                if let Ok(payload) = serde_json::to_string(&update_msg) {
                                    if let Err(e) = client.publish("coco/events", QoS::AtLeastOnce, false, payload).await {
                                        error!("Failed to publish object-created event for object '{}': {}", object_id, e);
                                    } else {
                                        trace!("Subscribing to MQTT topic for object '{}'", object_id);
                                        let mut filter = Filter::new(format!("coco/{}/#", object_id), QoS::AtLeastOnce);
                                        filter.nolocal = true;
                                        if let Err(e) = client.subscribe_many(vec![filter]).await {
                                            error!("Failed to subscribe to MQTT topic for object '{}': {}", object_id, e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    CoCoEvent::ClassesUpdated(object_id, classes) => {
                        let update_msg = serde_json::json!({
                            "msg_type": "classes-updated",
                            "object_id": object_id,
                            "classes": classes
                        });
                        let payload = serde_json::to_string(&update_msg).unwrap_or_default();
                        if let Err(e) = client.publish("coco/events", QoS::AtLeastOnce, false, payload).await {
                            error!("Failed to publish classes-updated event for object '{}': {}", object_id, e);
                        }
                    }
                    CoCoEvent::PropertiesUpdated(object_id, properties) => {
                        if let Ok(payload) = serde_json::to_string(&properties) {
                            if let Err(e) = client.publish(format!("coco/{}/static", object_id), QoS::AtLeastOnce, false, payload).await {
                                error!("Failed to publish properties-updated event for object '{}': {}", object_id, e);
                            }
                        }
                    }
                    CoCoEvent::ValuesAdded(object_id, values, timestamp) => {
                        let update_msg = serde_json::json!({
                            "values": values,
                            "timestamp": timestamp
                        });
                        if let Ok(payload) = serde_json::to_string(&update_msg) {
                            if let Err(e) = client.publish(format!("coco/{}/dynamic", object_id), QoS::AtLeastOnce, false, payload).await {
                                error!("Failed to publish values-added event for object '{}': {}", object_id, e);
                            }
                        }
                    }
                    CoCoEvent::RuleCreated(rule) => {
                        if let Ok(rule_data) = coco_clone.get_rule(rule.clone()).await {
                            if let Ok(mut update_msg) = serde_json::to_value(rule_data) {
                                update_msg["msg_type"] = serde_json::json!("rule-created");
                                if let Ok(payload) = serde_json::to_string(&update_msg) {
                                    if let Err(e) = client.publish("coco/events", QoS::AtLeastOnce, false, payload).await {
                                        error!("Failed to publish rule-created event for rule '{}': {}", rule, e);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        let broker = self.broker.clone();
        let port = self.port;
        tokio::spawn(async move {
            loop {
                let notification = match eventloop.poll().await {
                    Ok(n) => n,
                    Err(e) => {
                        error!("MQTT connection error: {:?}", e);
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        continue;
                    }
                };

                let Event::Incoming(Packet::Publish(publish)) = notification else {
                    if let Event::Incoming(Packet::ConnAck(_)) = notification {
                        info!("Connected to MQTT broker at {}:{}", broker, port);
                    }
                    continue;
                };

                let topic = String::from_utf8_lossy(&publish.topic);
                trace!("Received MQTT message on topic {}", topic);

                let topic_parts: Vec<&str> = topic.split('/').collect();
                if topic_parts.len() < 3 {
                    warn!("Received MQTT message with unexpected topic format: {}", topic);
                    continue;
                }

                let object_id = topic_parts[1].to_string();
                let msg_type = topic_parts[2];

                let classes = match coco.get_object_classes(object_id.clone()).await {
                    Ok(c) => c,
                    Err(CoCoError::ObjectNotFound(e)) => {
                        warn!("Object not found for ID '{}': {}", object_id, e);
                        continue;
                    }
                    Err(e) => {
                        warn!("Failed to get classes for object with ID '{}': {}", object_id, e);
                        continue;
                    }
                };

                match msg_type {
                    "static" => {
                        let values = match serde_json::from_slice::<Value>(&publish.payload) {
                            Ok(v) => v,
                            Err(e) => {
                                warn!("Failed to parse MQTT message payload as JSON: {}", e);
                                continue;
                            }
                        };
                        match properties_from_json(coco.clone(), classes, values).await {
                            Ok(properties) => {
                                if let Err(e) = coco.set_properties(object_id.clone(), properties).await {
                                    error!("Failed to set properties for object with ID '{}': {}", object_id, e);
                                }
                            }
                            Err(e) => error!("Failed to parse properties from MQTT message for object with ID '{}': {}", object_id, e),
                        }
                    }
                    "dynamic" => {
                        let (values, timestamp) = match serde_json::from_slice::<Value>(&publish.payload) {
                            Ok(v) => {
                                let timestamp = v.get("timestamp").and_then(|ts| ts.as_i64()).and_then(|secs| DateTime::<Utc>::from_timestamp(secs, 0)).unwrap_or_else(Utc::now);
                                if let Some(values) = v.get("values") {
                                    (values.clone(), timestamp)
                                } else {
                                    warn!("Received MQTT message without 'values' field");
                                    continue;
                                }
                            }
                            Err(e) => {
                                warn!("Failed to parse MQTT message payload as JSON: {}", e);
                                continue;
                            }
                        };

                        match values_from_json(coco.clone(), classes, values).await {
                            Ok(dynamic_values) => {
                                if let Err(e) = coco.add_values(object_id.clone(), dynamic_values, timestamp).await {
                                    error!("Failed to add values for object with ID '{}': {}", object_id, e);
                                }
                            }
                            Err(e) => error!("Failed to parse values from MQTT message for object with ID '{}': {}", object_id, e),
                        }
                    }
                    _ => {
                        warn!("Received MQTT message with unknown msg_type: {}", msg_type);
                        continue;
                    }
                }
            }
        });

        Ok(())
    }
}
