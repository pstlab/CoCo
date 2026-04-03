use std::{collections::HashMap, time::Duration};

use crate::{
    CoCo,
    model::{CoCoError, CoCoEvent, Value},
};
use chrono::{DateTime, Utc};
use rumqttc::v5::{
    AsyncClient, Event, MqttOptions,
    mqttbytes::{
        QoS,
        v5::{Filter, Packet},
    },
};
use tracing::{info, trace};

pub async fn setup_mqtt(coco: CoCo) -> Result<(), CoCoError> {
    let mqtt_broker = std::env::var("MQTT_BROKER").unwrap_or_else(|_| "localhost".to_string());
    let mqtt_port = std::env::var("MQTT_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(1883);
    add_mqtt(coco, mqtt_broker, mqtt_port).await
}

pub async fn add_mqtt(coco: CoCo, broker: String, port: u16) -> Result<(), CoCoError> {
    info!("Starting MQTT client connecting to {}:{}", broker, port);
    let mut mqtt_options = MqttOptions::new("coco-client-id", broker.as_str(), port);
    mqtt_options.set_keep_alive(Duration::from_secs(5));
    let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);

    for obj in coco.get_objects().await? {
        trace!("Subscribing to MQTT topic for existing object '{}'", obj.id.as_ref().unwrap());
        let mut filter = Filter::new(format!("coco/{}/#", obj.id.as_ref().unwrap()), QoS::AtLeastOnce);
        filter.nolocal = true;
        client.subscribe_many(vec![filter]).await.unwrap();
    }

    let mut rx = coco.event_tx.subscribe();
    let coco_clone = coco.clone();
    tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            match msg {
                CoCoEvent::ClassCreated(class_name) => match coco_clone.get_class(&class_name).await {
                    Ok(Some(class)) => {
                        let mut update_msg = serde_json::to_value(class).unwrap();
                        update_msg["msg_type"] = serde_json::json!("class-created");
                        let payload = serde_json::to_string(&update_msg).unwrap();
                        client.publish("coco/events", QoS::AtLeastOnce, false, payload).await.unwrap();
                    }
                    Ok(None) => trace!("Class '{}' not found after creation event", class_name),
                    Err(e) => trace!("Error loading class '{}' after creation event: {}", class_name, e),
                },
                CoCoEvent::ObjectCreated(object_id) => match coco_clone.get_object(&object_id).await {
                    Ok(Some(object)) => {
                        let mut update_msg = serde_json::to_value(object).unwrap();
                        update_msg["msg_type"] = serde_json::json!("object-created");
                        let payload = serde_json::to_string(&update_msg).unwrap();
                        client.publish("coco/events", QoS::AtLeastOnce, false, payload).await.unwrap();

                        trace!("Subscribing to MQTT topic for object '{}'", object_id);
                        let mut filter = Filter::new(format!("coco/{}/#", object_id), QoS::AtLeastOnce);
                        filter.nolocal = true;
                        client.subscribe_many(vec![filter]).await.unwrap();
                    }
                    Ok(None) => trace!("Object '{}' not found after creation event", object_id),
                    Err(e) => trace!("Error loading object '{}' after creation event: {}", object_id, e),
                },
                CoCoEvent::AddedClass(object_id, class_name) => {
                    let update_msg = serde_json::json!({
                        "msg_type": "added_class",
                        "object_id": object_id,
                        "class_name": class_name
                    });
                    let payload = serde_json::to_string(&update_msg).unwrap();
                    client.publish("coco/events", QoS::AtLeastOnce, false, payload).await.unwrap();
                }
                CoCoEvent::UpdatedProperties(object_id, properties) => {
                    let update_msg = serde_json::json!(properties);
                    let payload = serde_json::to_string(&update_msg).unwrap();
                    client.publish(format!("coco/{}/static", object_id), QoS::AtLeastOnce, false, payload).await.unwrap();
                }
                CoCoEvent::AddedValues(object_id, values, date_time) => {
                    let update_msg = serde_json::json!({
                        "values": values,
                        "date_time": date_time
                    });
                    let payload = serde_json::to_string(&update_msg).unwrap();
                    client.publish(format!("coco/{}/dynamic", object_id), QoS::AtLeastOnce, false, payload).await.unwrap();
                }
                CoCoEvent::RuleCreated(rule) => {
                    let mut update_msg = serde_json::to_value(coco_clone.get_rule(&rule).await.unwrap()).unwrap();
                    update_msg["msg_type"] = serde_json::json!("rule_created");
                    let payload = serde_json::to_string(&update_msg).unwrap();
                    client.publish("coco/events", QoS::AtLeastOnce, false, payload).await.unwrap();
                }
            }
        }
    });

    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(notification) => match notification {
                    Event::Incoming(Packet::ConnAck(_)) => info!("Connected to MQTT broker at {}:{}", broker, port),
                    Event::Incoming(Packet::Publish(publish)) => {
                        trace!("Received MQTT message on topic {}", String::from_utf8_lossy(&publish.topic));
                        let topic = String::from_utf8_lossy(&publish.topic).to_string();
                        let topic_parts: Vec<&str> = topic.split('/').collect();
                        let msg = String::from_utf8_lossy(&publish.payload).to_string();
                        if topic_parts[2] == "static" {
                            let data: HashMap<String, Value> = serde_json::from_str(&msg).unwrap();
                            coco.set_properties(topic_parts[1], data).await.unwrap();
                        } else if topic_parts[2] == "dynamic" {
                            let mut update: serde_json::Value = serde_json::from_str(&msg).unwrap();
                            let values: HashMap<String, Value> = serde_json::from_value(update["values"].take()).unwrap();
                            let date_time: DateTime<Utc> = if update.get("date_time").is_some() { serde_json::from_value(update["date_time"].take()).unwrap() } else { Utc::now() };
                            coco.add_values(topic_parts[1], values, date_time).await.unwrap();
                        }
                    }
                    _ => {}
                },
                Err(e) => eprintln!("Error: {:?}", e),
            }
        }
    });

    Ok(())
}
