use std::collections::{HashMap, HashSet};

use chrono::Utc;
use coco::{
    CoCo, CoCoModule,
    db::{Database, mongodb::MongoDB},
    kb::clips::CLIPSKnowledgeBase,
    model::{Class, Object, Property, Rule, Value},
};
use tracing::{Level, error, subscriber};

async fn create_coco() -> (CoCo, MongoDB) {
    let name: String = std::env::var("DB_NAME").unwrap_or_else(|_| "coco_test_db".to_owned());
    let host = std::env::var("DB_HOST").unwrap_or_else(|_| "localhost".to_owned());
    let port = std::env::var("DB_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(27017);
    let uri = format!("mongodb://{}:{}", host, port);

    let db = MongoDB::new(name, uri).await.unwrap_or_else(|e| {
        error!("Failed to set up MongoDB: {}", e);
        std::process::exit(1);
    });
    let (kb, event) = CLIPSKnowledgeBase::new();
    let modules: Vec<Box<dyn CoCoModule<MongoDB, CLIPSKnowledgeBase>>> = vec![
        #[cfg(feature = "ollama")]
        Box::new(OllamaModule::default()),
        #[cfg(feature = "fcm")]
        Box::new(FCMModule::default()),
        #[cfg(feature = "mqtt")]
        Box::new(MQTTModule::default()),
    ];

    (CoCo::new(db.clone(), kb, event, modules).await, db)
}

#[tokio::test]
async fn create_objects() {
    let subscriber = tracing_subscriber::fmt().with_max_level(Level::TRACE).finish();
    subscriber::set_global_default(subscriber).expect("Failed to set global default subscriber");

    let (coco, db) = create_coco().await;

    coco.create_class(Class {
        name: "Sensor".to_string(),
        parents: None,
        static_properties: None,
        dynamic_properties: None,
    })
    .await
    .unwrap();

    coco.create_class(Class {
        name: "TemperatureSensor".to_string(),
        parents: Some(HashSet::from(["Sensor".to_string()])),
        static_properties: None,
        dynamic_properties: Some(HashMap::from([("temperature".to_string(), Property::Float { default: None, min: Some(-10.0), max: Some(50.0) })])),
    })
    .await
    .unwrap();

    coco.create_class(Class {
        name: "Patient".to_string(),
        parents: None,
        static_properties: None,
        dynamic_properties: Some(HashMap::from([("sbp".to_string(), Property::Float { default: None, min: Some(80.0), max: Some(200.0) }), ("dbp".to_string(), Property::Float { default: None, min: Some(60.0), max: Some(120.0) })])),
    })
    .await
    .unwrap();

    coco.create_class(Class {
        name: "PhysiologicalSensor".to_string(),
        parents: Some(HashSet::from(["Sensor".to_string()])),
        static_properties: None,
        dynamic_properties: Some(HashMap::from([("patient".to_string(), Property::Object { default: None, class: "Patient".to_string() })])),
    })
    .await
    .unwrap();

    coco.create_class(Class {
        name: "BloodPressureSensor".to_string(),
        parents: Some(HashSet::from(["PhysiologicalSensor".to_string()])),
        static_properties: None,
        dynamic_properties: Some(HashMap::from([("sbp".to_string(), Property::Float { default: None, min: Some(80.0), max: Some(200.0) }), ("dbp".to_string(), Property::Float { default: None, min: Some(60.0), max: Some(120.0) })])),
    })
    .await
    .unwrap();

    coco.create_rule(Rule {
        name: "BloodPressureMeasurement".to_string(),
        content: r#"(defrule BloodPressureMeasurement
                        (PhysiologicalSensor_patient (id ?id) (value ?patient&~nil))
                        (BloodPressureSensor_sbp (id ?id) (value ?sbp))
                        (BloodPressureSensor_dbp (id ?id) (value ?dbp))
                        =>
                        (printout t "Measured blood pressure for patient " ?patient ": " ?sbp "/" ?dbp crlf)
                        (add-data ?patient (create$ sbp dbp) (create$ ?sbp ?dbp)))"#
            .to_string(),
    })
    .await
    .unwrap();

    let temperature_sensor_id = coco
        .create_object(Object {
            id: None,
            classes: HashSet::from(["TemperatureSensor".to_string()]),
            properties: None,
            values: None,
        })
        .await
        .unwrap();

    coco.add_values(temperature_sensor_id.clone(), HashMap::from([("temperature".to_string(), coco::model::Value::Float(22.5))]), Utc::now()).await.unwrap();

    let patient_id = coco.create_object(Object { id: None, classes: HashSet::from(["Patient".to_string()]), properties: None, values: None }).await.unwrap();

    let bp_sensor_id = coco
        .create_object(Object {
            id: None,
            classes: HashSet::from(["BloodPressureSensor".to_string()]),
            properties: None,
            values: None,
        })
        .await
        .unwrap();

    coco.add_values(bp_sensor_id.clone(), HashMap::from([("patient".to_string(), Value::Object(patient_id.clone())), ("sbp".to_string(), Value::Float(120.0)), ("dbp".to_string(), Value::Float(80.0))]), Utc::now()).await.unwrap();

    db.drop_database().await.unwrap();
}
