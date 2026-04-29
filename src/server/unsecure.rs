use crate::{
    CoCo,
    model::{Class, CoCoError, CoCoEvent, Object, Property, Rule, TimedValue, Value, object_from_json, properties_from_json, values_from_json},
    server::{DataFilter, DateQuery, ObjectFilter},
};
use axum::{
    Json, Router,
    extract::{
        Path, Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};
use chrono::Utc;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use tokio::sync::broadcast::error::RecvError;
use tracing::{error, trace};
use utoipa::OpenApi;

type OpenApiValue = Value;
type OpenApiObject = Object;

pub async fn unsecure_coco_router(coco: CoCo) -> Router {
    Router::new()
        .route("/ws", get(ws_handler))
        .route("/classes", get(get_classes).post(create_class))
        .route("/classes/{name}", get(get_class))
        .route("/rules", get(get_rules).post(create_rule))
        .route("/rules/{name}", get(get_rule))
        .route("/objects", get(get_objects).post(create_object))
        .route("/objects/{id}", get(get_object).patch(set_properties))
        .route("/objects/{id}/data", get(get_data).post(add_data))
        .route("/openapi", get(openapi))
        .with_state(coco)
}

#[utoipa::path(
        get,
        path = "/classes",
        tag = "Classes",
        summary = "List all classes",
        description = "Retrieve a list of all available classes in the knowledge base.",
        responses(
            (status = 200, description = "List of classes", body = [Class])
        )
    )]
async fn get_classes(State(coco): State<CoCo>) -> impl IntoResponse {
    trace!("Handling request to list all classes");
    match coco.get_classes().await {
        Ok(classes) => Json(classes).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to get classes: {}", e)).into_response(),
    }
}

#[utoipa::path(
        get,
        path = "/classes/{name}",
        tag = "Classes",
        summary = "Get a class",
        description = "Retrieve details for a specific class by its name.",
        params(
            ("name" = String, Path, description = "Name of the class to retrieve")
        ),
        responses(
            (status = 200, description = "The requested class", body = Class),
            (status = 404, description = "Class not found")
        )
    )]
async fn get_class(State(coco): State<CoCo>, Path(name): Path<String>) -> impl IntoResponse {
    trace!("Handling request to get class with name: {}", name);
    match coco.get_class(name.clone()).await {
        Ok(Some(class)) => Json(class).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, format!("Class '{}' not found", name)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to get class '{}': {}", name, e)).into_response(),
    }
}

#[utoipa::path(
        post,
        path = "/classes",
        tag = "Classes",
        summary = "Create a class",
        description = "Create a new class in the knowledge base.",
        request_body = Class,
        responses(
            (status = 201, description = "Class created successfully"),
            (status = 400, description = "Invalid class data in request body"),
            (status = 409, description = "Class already exists"),
            (status = 500, description = "Failed to create class")
        )
    )]
async fn create_class(State(coco): State<CoCo>, Json(class): Json<Class>) -> impl IntoResponse {
    trace!("Handling request to create class with name: {}", class.name);
    match coco.create_class(class).await {
        Ok(_) => StatusCode::CREATED.into_response(),
        Err(CoCoError::ClassAlreadyExists(_)) => (StatusCode::CONFLICT, "Class already exists".to_string()).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to create class: {}", e)).into_response(),
    }
}

#[utoipa::path(
        get,
        path = "/rules",
        tag = "Rules",
        summary = "List all rules",
        description = "Retrieve a list of all available rules in the knowledge base.",
        responses(
            (status = 200, description = "List of rules", body = [String])
        )
    )]
async fn get_rules(State(coco): State<CoCo>) -> impl IntoResponse {
    trace!("Handling request to list all rules");
    match coco.get_rules().await {
        Ok(rules) => Json(rules).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to get rules: {}", e)).into_response(),
    }
}

#[utoipa::path(
        get,
        path = "/rules/{name}",
        tag = "Rules",
        summary = "Get a rule",
        description = "Retrieve details for a specific rule by its name.",
        params(
            ("name" = String, Path, description = "Name of the rule to retrieve")
        ),
        responses(
            (status = 200, description = "The requested rule", body = String),
            (status = 404, description = "Rule not found")
        )
    )]
async fn get_rule(State(coco): State<CoCo>, Path(name): Path<String>) -> impl IntoResponse {
    trace!("Handling request to get rule with name: {}", name);
    match coco.get_rule(name.clone()).await {
        Ok(Some(rule)) => Json(rule).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, format!("Rule '{}' not found", name)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to get rule '{}': {}", name, e)).into_response(),
    }
}

#[utoipa::path(
        post,
        path = "/rules",
        tag = "Rules",
        summary = "Create a rule",
        description = "Create a new rule in the knowledge base.",
        request_body = Rule,
        responses(
            (status = 201, description = "Rule created successfully"),
            (status = 400, description = "Invalid rule data in request body"),
            (status = 409, description = "Rule already exists"),
            (status = 500, description = "Failed to create rule")
        )
    )]
async fn create_rule(State(coco): State<CoCo>, Json(rule): Json<Rule>) -> impl IntoResponse {
    trace!("Handling request to create rule with name: {}", rule.name);
    match coco.create_rule(rule).await {
        Ok(_) => StatusCode::CREATED.into_response(),
        Err(CoCoError::RuleAlreadyExists(_)) => (StatusCode::CONFLICT, "Rule already exists".to_string()).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to create rule: {}", e)).into_response(),
    }
}

#[utoipa::path(
        get,
        path = "/objects",
        tag = "Objects",
        summary = "List all objects",
        description = "Retrieve a list of all available objects in the knowledge base.",
        params(ObjectFilter),
        responses(
            (status = 200, description = "List of objects", body = [OpenApiObject])
        )
    )]
async fn get_objects(State(coco): State<CoCo>, Query(filter): Query<ObjectFilter>) -> impl IntoResponse {
    trace!("Handling request to list all objects with filter: {:?}", filter);
    match coco.get_objects().await {
        Ok(objects) => {
            let filtered_objects: Vec<Object> = objects
                .into_iter()
                .filter(|o| {
                    if !filter.class.as_ref().is_none_or(|class_name| o.classes.contains(class_name)) {
                        return false;
                    }
                    if !filter.extra.as_ref().is_none_or(|extra| extra.iter().all(|(k, v)| o.properties.as_ref().and_then(|props| props.get(k)).is_none_or(|prop| prop == v))) {
                        return false;
                    }
                    true
                })
                .collect();
            Json(filtered_objects).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to get objects: {}", e)).into_response(),
    }
}

#[utoipa::path(
        get,
        path = "/objects/{id}",
        tag = "Objects",
        summary = "Get an object",
        description = "Retrieve details for a specific object by its ID.",
        params(
            ("id" = String, Path, description = "ID of the object to retrieve")
        ),
        responses(
            (status = 200, description = "The requested object", body = OpenApiObject),
            (status = 404, description = "Object not found")
        )
    )]
async fn get_object(State(coco): State<CoCo>, Path(id): Path<String>) -> impl IntoResponse {
    trace!("Handling request to get object with ID: {}", id);
    match coco.get_object(id.clone()).await {
        Ok(Some(object)) => Json(object).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, format!("Object with ID '{}' not found", id)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to get object with ID '{}': {}", id, e)).into_response(),
    }
}

#[utoipa::path(
        post,
        path = "/objects",
        tag = "Objects",
        summary = "Create an object",
        description = "Create a new object in the knowledge base.",
        request_body = OpenApiObject,
        responses(
            (status = 201, description = "Object created successfully", body = String),
            (status = 400, description = "Invalid object data in request body"),
            (status = 404, description = "Class not found for object"),
            (status = 409, description = "Object already exists"),
            (status = 500, description = "Failed to create object")
        )
    )]
async fn create_object(State(coco): State<CoCo>, Json(object): Json<JsonValue>) -> impl IntoResponse {
    trace!("Handling request to create object: {:?}", object);
    match object_from_json(coco.clone(), object).await {
        Ok(new_object) => match coco.create_object(new_object).await {
            Ok(object_id) => (StatusCode::CREATED, object_id).into_response(),
            Err(CoCoError::ClassNotFound(e)) => (StatusCode::NOT_FOUND, format!("Class not found: {}", e)).into_response(),
            Err(CoCoError::ObjectAlreadyExists(e)) => (StatusCode::CONFLICT, format!("Object already exists: {}", e)).into_response(),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to create object: {}", e)).into_response(),
        },
        Err(e) => (StatusCode::BAD_REQUEST, format!("Invalid object data in request body: {}", e)).into_response(),
    }
}

#[utoipa::path(
        patch,
        path = "/objects/{id}",
        tag = "Objects",
        summary = "Set object properties",
        description = "Update the properties of an existing object.",
        params(
            ("id" = String, Path, description = "ID of the object to update")
        ),
        request_body = inline(HashMap<String, OpenApiValue>),
        responses(
            (status = 200, description = "Object properties updated successfully"),
            (status = 400, description = "Invalid property values in request body"),
            (status = 404, description = "Object not found"),
            (status = 500, description = "Failed to update object properties")
        )
    )]
async fn set_properties(State(coco): State<CoCo>, Path(object_id): Path<String>, Json(properties): Json<JsonValue>) -> impl IntoResponse {
    trace!("Handling request to set properties for object with ID: {}. New properties: {:?}", object_id, properties);
    match coco.get_object_classes(object_id.clone()).await {
        Ok(classes) => match properties_from_json(coco.clone(), classes, properties).await {
            Ok(properties) => match coco.set_properties(object_id.clone(), properties).await {
                Ok(_) => StatusCode::OK.into_response(),
                Err(CoCoError::ObjectNotFound(e)) => (StatusCode::NOT_FOUND, format!("Object not found: {}", e)).into_response(),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to update properties for object with ID '{}': {}", object_id, e)).into_response(),
            },
            Err(e) => (StatusCode::BAD_REQUEST, format!("Invalid property values in request body: {}", e)).into_response(),
        },
        Err(CoCoError::ObjectNotFound(e)) => (StatusCode::NOT_FOUND, format!("Object not found: {}", e)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to retrieve object with ID '{}': {}", object_id, e)).into_response(),
    }
}

#[utoipa::path(
        post,
        path = "/objects/{id}/data",
        tag = "Objects",
        summary = "Add data to an object",
        description = "Add new data values to an existing object.",
        params(
            ("id" = String, Path, description = "ID of the object to update"),
            ("time" = Option<DateTime<Utc>>, Query, description = "Timestamp for the data being added (optional, defaults to current time)")
        ),
        request_body = inline(HashMap<String, OpenApiValue>),
        responses(
            (status = 200, description = "Data added to object successfully"),
            (status = 400, description = "Invalid data values in request body"),
            (status = 404, description = "Object not found"),
            (status = 500, description = "Failed to add data to object")
        )
    )]
async fn add_data(State(coco): State<CoCo>, Path(object_id): Path<String>, Query(date_time): Query<DateQuery>, Json(values): Json<JsonValue>) -> impl IntoResponse {
    trace!("Handling request to add data to object with ID: {}. Values: {:?}, Timestamp: {:?}", object_id, values, date_time);
    let timestamp = date_time.time.unwrap_or_else(Utc::now);
    match coco.get_object_classes(object_id.clone()).await {
        Ok(classes) => match values_from_json(coco.clone(), classes, values).await {
            Ok(values) => match coco.add_values(object_id.clone(), values, timestamp).await {
                Ok(_) => StatusCode::OK.into_response(),
                Err(CoCoError::ObjectNotFound(e)) => (StatusCode::NOT_FOUND, format!("Object not found: {}", e)).into_response(),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to add data to object with ID '{}': {}", object_id, e)).into_response(),
            },
            Err(e) => (StatusCode::BAD_REQUEST, format!("Invalid data values in request body: {}", e)).into_response(),
        },
        Err(CoCoError::ObjectNotFound(e)) => (StatusCode::NOT_FOUND, format!("Object not found: {}", e)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to retrieve object with ID '{}': {}", object_id, e)).into_response(),
    }
}

#[utoipa::path(
        get,
        path = "/objects/{id}/data",
        tag = "Objects",
        summary = "Get object data",
        description = "Retrieve data values for a specific object, optionally filtered by a time range.",
        params(
            ("id" = String, Path, description = "ID of the object to retrieve data for"),
            ("start" = Option<DateTime<Utc>>, Query, description = "Start of the time range filter (optional)"),
            ("end" = Option<DateTime<Utc>>, Query, description = "End of the time range filter (optional)")
        ),
        responses(
            (status = 200, description = "List of data values for the object", body = [HashMap<String, Value>]),
            (status = 404, description = "Object not found"),
            (status = 500, description = "Failed to retrieve object data")
        )
    )]
async fn get_data(State(coco): State<CoCo>, Path(object_id): Path<String>, Query(filter): Query<DataFilter>) -> impl IntoResponse {
    trace!("Handling request to get data for object with ID: {}. Time filter: {:?}", object_id, filter);
    match coco.get_values(object_id.clone(), filter.start, filter.end).await {
        Ok(data) => {
            let mut result: HashMap<String, Vec<TimedValue>> = HashMap::new();
            for (map, timestamp) in data {
                for (key, value) in map {
                    result.entry(key).or_default().push(TimedValue { value, timestamp });
                }
            }
            Json(result).into_response()
        }
        Err(CoCoError::ObjectNotFound(e)) => (StatusCode::NOT_FOUND, format!("Object not found: {}", e)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to get data for object with ID '{}': {}", object_id, e)).into_response(),
    }
}

#[utoipa::path(
        get,
        path = "/ws",
        tag = "System",
        summary = "WebSocket connection",
        description = "Establish a WebSocket connection for real-time updates.",
        responses(
            (status = 101, description = "WebSocket connection established"),
        )
    )]
async fn ws_handler(ws: WebSocketUpgrade, State(state): State<CoCo>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| async move { handle_socket(socket, state).await })
}

async fn handle_socket(mut socket: WebSocket, coco: CoCo) {
    trace!("WebSocket connection established");

    let init_msg = match async {
        let classes_map: HashMap<String, serde_json::Value> = coco
            .get_classes()
            .await?
            .into_iter()
            .map(|mut c| {
                let name = std::mem::take(&mut c.name);
                let mut v = serde_json::to_value(&c).unwrap();
                v.as_object_mut().unwrap().remove("name");
                (name, v)
            })
            .collect();

        let rules_map: HashMap<String, serde_json::Value> = coco
            .get_rules()
            .await?
            .into_iter()
            .map(|mut r| {
                let name = std::mem::take(&mut r.name);
                let mut v = serde_json::to_value(&r).unwrap();
                v.as_object_mut().unwrap().remove("name");
                (name, v)
            })
            .collect();

        let objects_map: HashMap<String, serde_json::Value> = coco
            .get_objects()
            .await?
            .into_iter()
            .map(|mut o| {
                let id = o.id.take().unwrap();
                let mut v = serde_json::to_value(&o).unwrap();
                v.as_object_mut().unwrap().remove("id");
                (id, v)
            })
            .collect();

        Ok::<serde_json::Value, CoCoError>(serde_json::json!({
            "msg_type": "coco",
            "classes": classes_map,
            "rules": rules_map,
            "objects": objects_map
        }))
    }
    .await
    {
        Ok(v) => v,
        Err(e) => {
            error!("Failed to build websocket init payload: {}", e);
            return;
        }
    };
    socket.send(Message::Text(serde_json::to_string(&init_msg).unwrap().into())).await.ok();

    let mut rx = coco.event_tx.subscribe();
    loop {
        tokio::select! {
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(payload))) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
            recv = rx.recv() => {
                let event = match recv {
                    Ok(event) => event,
                    Err(RecvError::Lagged(skipped)) => {
                        trace!("WebSocket client lagging behind, skipped {} events", skipped);
                        continue;
                    }
                    Err(RecvError::Closed) => break,
                };
                let send_result = match event {
                    CoCoEvent::ClassCreated(class_name) => match coco.get_class(class_name).await {
                        Ok(Some(class)) => {
                            let mut update_msg = serde_json::to_value(class).unwrap();
                            update_msg["msg_type"] = serde_json::json!("class-created");
                            socket.send(Message::Text(serde_json::to_string(&update_msg).unwrap().into())).await
                        }
                        Ok(None) => Ok(()),
                        Err(_) => Ok(()),
                    },
                    CoCoEvent::RuleCreated(rule) => match coco.get_rule(rule).await {
                        Ok(Some(rule)) => {
                            let mut update_msg = serde_json::to_value(rule).unwrap();
                            update_msg["msg_type"] = serde_json::json!("rule-created");
                            socket.send(Message::Text(serde_json::to_string(&update_msg).unwrap().into())).await
                        }
                        Ok(None) => Ok(()),
                        Err(_) => Ok(()),
                    },
                    CoCoEvent::ObjectCreated(object_id) => match coco.get_object(object_id).await {
                        Ok(Some(object)) => {
                            let mut update_msg = serde_json::to_value(object).unwrap();
                            update_msg["msg_type"] = serde_json::json!("object-created");
                            socket.send(Message::Text(serde_json::to_string(&update_msg).unwrap().into())).await
                        }
                        Ok(None) => Ok(()),
                        Err(_) => Ok(()),
                    },
                    CoCoEvent::AddedClass(object_id, class_name) => {
                        let update_msg = serde_json::json!({
                            "msg_type": "added-class",
                            "object_id": object_id,
                            "class_name": class_name
                        });
                        socket.send(Message::Text(serde_json::to_string(&update_msg).unwrap().into())).await
                    }
                    CoCoEvent::UpdatedProperties(object_id, properties) => {
                        let update_msg = serde_json::json!({
                            "msg_type": "updated-properties",
                            "object_id": object_id,
                            "properties": properties
                        });
                        socket.send(Message::Text(serde_json::to_string(&update_msg).unwrap().into())).await
                    }
                    CoCoEvent::AddedValues(object_id, values, date_time) => {
                        let update_msg = serde_json::json!({
                            "msg_type": "added-values",
                            "object_id": object_id,
                            "values": values,
                            "date_time": date_time
                        });
                        socket.send(Message::Text(serde_json::to_string(&update_msg).unwrap().into())).await
                    }
                };

                // If sending fails (e.g., client disconnected), break out of the loop
                if send_result.is_err() {
                    break;
                }
            }
        }
    }
}

#[utoipa::path(
        get,
        path = "/openapi",
        tag = "System",
        summary = "Get OpenAPI spec",
        description = "Retrieve the OpenAPI specification for this API.",
        responses(
            (status = 200, description = "OpenAPI specification in JSON format", body = String)
        )
    )]
async fn openapi() -> impl IntoResponse {
    Json(ApiDoc::openapi())
}

#[derive(OpenApi)]
#[openapi(
    servers(
        (url = "/", description = "Base URL for CoCo API")
    ),
    paths(get_classes, get_class, create_class, get_rules, get_rule, create_rule, get_objects, get_object, create_object, set_properties, add_data, get_data, ws_handler, openapi),
    components(
        schemas(Class, Rule, Property, OpenApiObject, OpenApiValue)
    ),
    tags(
        (name = "Classes", description = "Operations related to knowledge base classes"),
        (name = "Objects", description = "Operations related to knowledge base objects"),
        (name = "Rules", description = "Operations related to knowledge base rules"),
        (name = "System", description = "System and utility endpoints")
    )
)]
pub struct ApiDoc;
