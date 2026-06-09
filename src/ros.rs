use crate::{
    CoCo, CoCoModule,
    db::Database,
    kb::KnowledgeBase,
    model::{Class, CoCoError, CoCoEvent, Property},
};
use async_trait::async_trait;
use dust_dds::{
    domain::domain_participant_factory::DomainParticipantFactory,
    infrastructure::{qos::QosKind, status::NO_STATUS},
    listener::NO_LISTENER,
    topic_definition::topic_description::TopicDescription,
    xtypes::{
        data_storage::DataStorage,
        dynamic_type::{DynamicType, DynamicTypeBuilderFactory, ExtensibilityKind, MemberDescriptor, TryConstructKind, TypeDescriptor, TypeKind},
    },
};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, trace};

pub struct ROSModule {}

impl ROSModule {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl<DB: Database, KB: KnowledgeBase> CoCoModule<DB, KB> for ROSModule {
    async fn init(&self, _db: DB, _kb: KB, coco: CoCo) -> Result<(), CoCoError> {
        let mut rx = coco.event_tx.subscribe();
        let coco_clone = coco.clone();
        let participant = DomainParticipantFactory::get_instance().create_participant(0, QosKind::Default, NO_LISTENER, NO_STATUS).expect("Failed to create ROS Domain Participant");
        tokio::spawn(async move {
            let mut types = HashMap::new();
            let mut topics: HashMap<String, HashMap<String, TopicDescription>> = HashMap::new();
            while let Ok(msg) = rx.recv().await {
                match msg {
                    CoCoEvent::ClassCreated(class_name) => {
                        let class = coco_clone.get_class(class_name.clone()).await.expect("Failed to get class after creation event").expect("Class not found after creation event");
                        if let Some(props) = class.dynamic_properties {
                            trace!("Creating ROS dynamic type for class '{}'", class_name);
                            let mut builder = DynamicTypeBuilderFactory::create_type(type_descriptor(&class_name));
                            let mut props_names = props.keys().cloned().collect::<Vec<_>>();
                            props_names.sort_unstable();
                            for prop_name in props_names {
                                builder.add_member(prop_descriptor(prop_name.clone(), 0, props.get(&prop_name).unwrap())).map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to add dynamic member: {:?}", e))).expect("Failed to add member to ROS dynamic type for class creation event");
                            }
                            types.insert(class_name, builder.build());
                        }
                    }
                    CoCoEvent::ObjectCreated(object_id) => {
                        let object = coco_clone.get_object(object_id.clone()).await.expect("Failed to get object after creation event").expect("Object not found after creation event");
                        for class_name in object.classes {
                            if let Some(tp) = types.get(&class_name) {
                                let topic_name = format!("rt/{}_{}", class_name, object_id);
                                trace!("Creating ROS topic '{}' for object '{}'", topic_name, object_id);
                                let topic = participant.create_dynamic_topic(topic_name.as_str(), &tp.get_name(), QosKind::Default, NO_LISTENER, NO_STATUS, Arc::new(tp.clone())).expect("Failed to create ROS topic for object creation event");
                                topics.entry(object_id.clone()).or_default().insert(class_name.clone(), topic);
                            }
                        }
                    }
                    CoCoEvent::ClassesUpdated(object_id, classes) => {
                        for class_name in topics.get(&object_id).unwrap_or(&HashMap::new()).keys() {
                            if !classes.contains(class_name) {
                                let topic = topics.get(&object_id).unwrap().get(class_name).unwrap();
                                trace!("Deleting ROS topic '{}' for object '{}'", topic.get_name(), object_id);
                                participant.delete_topic(topic).expect("Failed to delete ROS topic for classes updated event");
                            }
                        }
                        for class_name in classes {
                            if let Some(tp) = types.get(&class_name) {
                                let topic_name = format!("rt/{}_{}", class_name, object_id);
                                trace!("Creating ROS topic '{}' for object '{}'", topic_name, object_id);
                                let topic = participant.create_dynamic_topic(topic_name.as_str(), &tp.get_name(), QosKind::Default, NO_LISTENER, NO_STATUS, Arc::new(tp.clone())).expect("Failed to create ROS topic for classes updated event");
                                topics.entry(object_id.clone()).or_default().insert(class_name.clone(), topic);
                            }
                        }
                    }
                    CoCoEvent::PropertiesUpdated(_object_id, _properties) => {}
                    CoCoEvent::ValuesAdded(object_id, values, date_time) => {}
                    CoCoEvent::RuleCreated(_rule) => {}
                }
            }
        });
        Ok(())
    }
}

fn type_descriptor(class_name: &String) -> TypeDescriptor {
    TypeDescriptor {
        kind: TypeKind::STRUCTURE,
        name: "coco::msg::dds_::".to_string() + class_name + "_",
        base_type: None,
        discriminator_type: None,
        bound: vec![],
        element_type: None,
        key_element_type: None,
        extensibility_kind: ExtensibilityKind::Final,
        is_nested: false,
    }
}

fn prop_descriptor(prop_name: String, id: u32, prop: &Property) -> MemberDescriptor {
    let (r#type, default_value) = prop_type_and_default(prop);

    MemberDescriptor {
        name: prop_name,
        id,
        r#type,
        is_optional: true,
        default_value,
        index: id,
        label: vec![],
        try_construct_kind: TryConstructKind::UseDefault,
        is_key: false,
        is_must_understand: false,
        is_shared: false,
        is_default_label: false,
    }
}

fn prop_type_and_default(prop: &Property) -> (DynamicType, Option<DataStorage>) {
    match prop {
        Property::Bool { default } => (DynamicTypeBuilderFactory::get_primitive_type(TypeKind::BOOLEAN), default.map(DataStorage::Boolean)),
        Property::Int { default, .. } => (DynamicTypeBuilderFactory::get_primitive_type(TypeKind::INT32), default.map(|v| DataStorage::Int32(v as i32))),
        Property::Float { default, .. } => (DynamicTypeBuilderFactory::get_primitive_type(TypeKind::FLOAT32), default.map(|v| DataStorage::Float32(v as f32))),
        Property::String { default } => (DynamicTypeBuilderFactory::get_primitive_type(TypeKind::STRING8), default.clone().map(DataStorage::String)),
        Property::Symbol { default, .. } => (DynamicTypeBuilderFactory::get_primitive_type(TypeKind::STRING8), default.clone().map(DataStorage::String)),
        Property::Object { default, .. } => (DynamicTypeBuilderFactory::get_primitive_type(TypeKind::STRING8), default.clone().map(DataStorage::String)),
        Property::BoolArray { default } => (
            DynamicTypeBuilderFactory::create_sequence_type(
                DynamicTypeBuilderFactory::get_primitive_type(TypeKind::BOOLEAN),
                0, // unbounded
            )
            .build(),
            default.as_ref().map(|arr| DataStorage::SequenceBoolean(arr.clone())),
        ),
        Property::IntArray { default, .. } => (
            DynamicTypeBuilderFactory::create_sequence_type(
                DynamicTypeBuilderFactory::get_primitive_type(TypeKind::INT32),
                0, // unbounded
            )
            .build(),
            default.as_ref().map(|arr| DataStorage::SequenceInt32(arr.iter().map(|&v| v as i32).collect())),
        ),
        Property::FloatArray { default, .. } => (
            DynamicTypeBuilderFactory::create_sequence_type(
                DynamicTypeBuilderFactory::get_primitive_type(TypeKind::FLOAT32),
                0, // unbounded
            )
            .build(),
            default.as_ref().map(|arr| DataStorage::SequenceFloat32(arr.iter().map(|&v| v as f32).collect())),
        ),
        Property::StringArray { default, .. } => (
            DynamicTypeBuilderFactory::create_sequence_type(
                DynamicTypeBuilderFactory::get_primitive_type(TypeKind::STRING8),
                0, // unbounded
            )
            .build(),
            default.as_ref().map(|arr| DataStorage::SequenceString(arr.clone())),
        ),
        Property::SymbolArray { default, .. } => (
            DynamicTypeBuilderFactory::create_sequence_type(
                DynamicTypeBuilderFactory::get_primitive_type(TypeKind::STRING8),
                0, // unbounded
            )
            .build(),
            default.as_ref().map(|arr| DataStorage::SequenceString(arr.clone())),
        ),
        Property::ObjectArray { default, .. } => (
            DynamicTypeBuilderFactory::create_sequence_type(
                DynamicTypeBuilderFactory::get_primitive_type(TypeKind::STRING8),
                0, // unbounded
            )
            .build(),
            default.as_ref().map(|arr| DataStorage::SequenceString(arr.clone())),
        ),
    }
}
