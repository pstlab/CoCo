use crate::{
    CoCo,
    db::DatabaseError,
    model::{Class, CoCoError, CoCoEvent, Object, Property, Rule, TimedValue, Value, object_from_json, properties_from_json, values_from_json},
    server::{
        DataFilter, DateQuery, ObjectFilter,
        secure_db::{Role, UserResponse, UsersDB},
    },
};
use axum::{
    Extension, Json, Router,
    extract::{
        Path, Query, Request, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{StatusCode, header},
    middleware::{Next, from_fn_with_state},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{Duration, Utc};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, errors::Error};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};
use tokio::sync::broadcast::error::RecvError;
use tracing::{error, trace};
use utoipa::{
    Modify, OpenApi, ToSchema,
    openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme},
};

type OpenApiValue = Value;
type OpenApiObject = Object;

#[derive(Clone)]
struct AppState {
    coco: CoCo,
    users_db: UsersDB,
}

pub async fn secure_coco_router(coco: CoCo, users_db: UsersDB) -> Router {
    let state = AppState { coco, users_db };

    let auth_router = Router::new()
        .route("/users", get(get_users).patch(update_user).post(create_user))
        .route("/classes", get(get_classes).post(create_class))
        .route("/classes/{name}", get(get_class))
        .route("/rules", get(get_rules).post(create_rule))
        .route("/rules/{name}", get(get_rule))
        .route("/objects", get(get_objects).post(create_object))
        .route("/objects/{id}", get(get_object).patch(set_properties))
        .route("/objects/{id}/data", get(get_data).post(add_data))
        .route("/openapi", get(openapi))
        .route_layer(from_fn_with_state(state.clone(), auth_middleware));

    let unauth_router = Router::new().route("/register", post(register)).route("/login", post(login)).route("/refresh_token", post(refresh_token)).route("/ws", get(ws_handler));

    unauth_router.merge(auth_router).with_state(state)
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
enum TokenType {
    #[default]
    Access,
    Refresh,
}

#[derive(Debug, Clone)]
struct CurrentUser {
    role: Role,
    read_access: Option<HashSet<String>>,
    write_access: Option<HashSet<String>>,
}

impl CurrentUser {
    fn has_read_access(&self, object_id: &str) -> bool {
        self.role == Role::Admin || self.read_access.as_ref().is_some_and(|access| access.contains(object_id))
    }

    fn has_write_access(&self, object_id: &str) -> bool {
        self.role == Role::Admin || self.write_access.as_ref().is_some_and(|access| access.contains(object_id))
    }
}

async fn auth_middleware(State(state): State<AppState>, mut req: Request, next: Next) -> Result<Response, StatusCode> {
    let header = req.headers().get(header::AUTHORIZATION).and_then(|h| h.to_str().ok());
    if let Some(token) = header.and_then(|h| h.strip_prefix("Bearer "))
        && let Ok(claims) = verify_jwt(token, state.users_db.secret())
        && claims.token_type == TokenType::Access
    {
        let user = state.users_db.get_user_by_username(&claims.sub).await.map_err(|_| StatusCode::UNAUTHORIZED)?;
        req.extensions_mut().insert(CurrentUser { role: user.role, read_access: user.read_access, write_access: user.write_access });
        return Ok(next.run(req).await);
    }
    Err(StatusCode::UNAUTHORIZED)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    sub: String,
    exp: usize,
    role: Role,
    #[serde(default)]
    token_type: TokenType,
}

#[derive(Serialize, ToSchema)]
struct AuthTokens {
    access_token: String,
    refresh_token: String,
    token_type: String,
}

pub fn create_jwt(user_id: &str, role: &Role, secret: &str) -> Result<String, Error> {
    let now = Utc::now();
    let expire = now + Duration::hours(24);

    let claims = Claims {
        sub: user_id.to_owned(),
        exp: expire.timestamp() as usize,
        role: role.clone(),
        token_type: TokenType::Access,
    };

    jsonwebtoken::encode(&Header::default(), &claims, &EncodingKey::from_secret(secret.as_ref()))
}

pub fn create_refresh_jwt(user_id: &str, role: &Role, secret: &str) -> Result<String, Error> {
    let now = Utc::now();
    let expire = now + Duration::days(30);

    let claims = Claims {
        sub: user_id.to_owned(),
        exp: expire.timestamp() as usize,
        role: role.clone(),
        token_type: TokenType::Refresh,
    };

    jsonwebtoken::encode(&Header::default(), &claims, &EncodingKey::from_secret(secret.as_ref()))
}

pub fn verify_jwt(token: &str, secret: &str) -> Result<Claims, Error> {
    let decoding_key = DecodingKey::from_secret(secret.as_ref());
    let validation = Validation::default();
    let token_data = jsonwebtoken::decode::<Claims>(token, &decoding_key, &validation)?;
    Ok(token_data.claims)
}

fn issue_tokens(username: &str, role: &Role, secret: &str) -> Result<AuthTokens, StatusCode> {
    let access_token = create_jwt(username, role, secret).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let refresh_token = create_refresh_jwt(username, role, secret).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(AuthTokens { access_token, refresh_token, token_type: "Bearer".to_owned() })
}

#[derive(Deserialize, ToSchema)]
struct Credentials {
    username: String,
    password: String,
}

#[utoipa::path(
        post,
        path = "/login",
        tag = "Authentication",
        summary = "Login a user",
        description = "Authenticate a user with their username and password, returns access and refresh JWT tokens if successful.",
        request_body = Credentials,
        responses(
            (status = 200, description = "User authenticated successfully, returns access and refresh JWT tokens", body = AuthTokens),
            (status = 401, description = "Invalid username or password"),
            (status = 500, description = "Failed to authenticate user")
        )
    )]
async fn login(State(state): State<AppState>, Json(req): Json<Credentials>) -> impl IntoResponse {
    match state.users_db.get_user(&req.username, &req.password).await {
        Ok(user) => issue_tokens(&user.username, &user.role, &state.users_db.secret()).map(Json),
        Err(DatabaseError::NotFound(_)) => Err(StatusCode::UNAUTHORIZED),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

#[utoipa::path(
        post,
        path = "/register",
        tag = "Authentication",
        summary = "Register a new user",
        description = "Create a new user account with a username, password, and role.",
        request_body = Credentials,
        responses(
            (status = 200, description = "User registered successfully, returns access and refresh JWT tokens", body = AuthTokens),
            (status = 409, description = "Username already exists"),
            (status = 500, description = "Failed to register user")
        )
    )]
async fn register(State(state): State<AppState>, Json(req): Json<Credentials>) -> impl IntoResponse {
    match state.users_db.create_user(&req.username, &req.password, Role::User, None, None).await {
        Ok(_) => match state.users_db.get_user(&req.username, &req.password).await {
            Ok(user) => issue_tokens(&user.username, &user.role, &state.users_db.secret()).map(Json),
            Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
        },
        Err(DatabaseError::Exists(_)) => Err(StatusCode::CONFLICT),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

#[derive(Deserialize, ToSchema)]
struct RefreshTokenRequest {
    refresh_token: String,
}

#[utoipa::path(
        post,
        path = "/refresh_token",
        tag = "Authentication",
        summary = "Refresh authentication tokens",
        description = "Exchange a valid refresh token for a new access and refresh JWT token pair.",
        request_body = RefreshTokenRequest,
        responses(
            (status = 200, description = "Tokens refreshed successfully", body = AuthTokens),
            (status = 401, description = "Invalid or expired refresh token"),
            (status = 500, description = "Failed to refresh tokens")
        )
    )]
async fn refresh_token(State(state): State<AppState>, Json(req): Json<RefreshTokenRequest>) -> impl IntoResponse {
    match verify_jwt(&req.refresh_token, &state.users_db.secret()) {
        Ok(claims) if claims.token_type == TokenType::Refresh => issue_tokens(&claims.sub, &claims.role, &state.users_db.secret()).map(Json).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR),
        _ => Err(StatusCode::UNAUTHORIZED),
    }
}

#[utoipa::path(
        get,
        path = "/users",
        tag = "Authentication",
        summary = "List all users",
        description = "Retrieve a list of all registered users (admin only).",
        security(("bearerAuth" = [])),
        responses(
            (status = 200, description = "List of users", body = [UserResponse]),
            (status = 401, description = "Missing or invalid JWT token"),
            (status = 403, description = "Forbidden - only admin users can view the list of users"),
            (status = 500, description = "Failed to retrieve users")
        )
    )]
async fn get_users(State(state): State<AppState>, Extension(user): Extension<CurrentUser>) -> impl IntoResponse {
    if user.role != Role::Admin {
        return (StatusCode::FORBIDDEN, "Only admin users can view the list of users").into_response();
    }
    match state.users_db.get_users().await {
        Ok(users) => (StatusCode::OK, axum::Json(users)).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Failed to retrieve users").into_response(),
    }
}

#[derive(Deserialize, ToSchema)]
struct CreateUserRequest {
    username: String,
    password: String,
    #[serde(default)]
    role: Role,
    #[serde(skip_serializing_if = "Option::is_none")]
    read_access: Option<HashSet<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    write_access: Option<HashSet<String>>,
}

#[utoipa::path(
        post,
        path = "/users",
        tag = "Authentication",
        summary = "Create a new user",
        description = "Create a new user account with a username, password, and role (admin only). Role defaults to user.",
        request_body = CreateUserRequest,
        security(("bearerAuth" = [])),
        responses(
            (status = 201, description = "User created successfully"),
            (status = 400, description = "Invalid user data in request body"),
            (status = 401, description = "Missing or invalid JWT token"),
            (status = 403, description = "Forbidden - only admin users can create new users"),
            (status = 409, description = "Username already exists"),
            (status = 500, description = "Failed to create user")
        )
    )]
async fn create_user(State(state): State<AppState>, Extension(user): Extension<CurrentUser>, Json(req): Json<CreateUserRequest>) -> impl IntoResponse {
    if user.role != Role::Admin {
        return (StatusCode::FORBIDDEN, "Only admin users can create new users").into_response();
    }
    match state.users_db.create_user(&req.username, &req.password, req.role, req.read_access, req.write_access).await {
        Ok(_) => (StatusCode::CREATED, "User created successfully").into_response(),
        Err(DatabaseError::Exists(_)) => (StatusCode::CONFLICT, "Username already exists").into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Failed to create user").into_response(),
    }
}

#[derive(Deserialize, ToSchema)]
struct UpdateUserRequest {
    username: String,
    #[serde(default)]
    role: Role,
    #[serde(skip_serializing_if = "Option::is_none")]
    read_access: Option<HashSet<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    write_access: Option<HashSet<String>>,
}

#[utoipa::path(
        patch,
        path = "/users",
        tag = "Authentication",
        summary = "Update a user",
        description = "Update an existing user's role and permissions (admin only).",
        request_body = UpdateUserRequest,
        security(("bearerAuth" = [])),
        responses(
            (status = 200, description = "User updated successfully"),
            (status = 400, description = "Invalid user data in request body"),
            (status = 401, description = "Missing or invalid JWT token"),
            (status = 403, description = "Forbidden - only admin users can update users"),
            (status = 404, description = "User not found"),
            (status = 500, description = "Failed to update user")
        )
    )]
async fn update_user(State(state): State<AppState>, Extension(user): Extension<CurrentUser>, Json(req): Json<UpdateUserRequest>) -> impl IntoResponse {
    if user.role != Role::Admin {
        return (StatusCode::FORBIDDEN, "Only admin users can update users").into_response();
    }
    match state.users_db.update_user(&req.username, req.role, req.read_access, req.write_access).await {
        Ok(_) => (StatusCode::OK, "User updated successfully").into_response(),
        Err(DatabaseError::NotFound(_)) => (StatusCode::NOT_FOUND, "User not found").into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Failed to update user").into_response(),
    }
}

#[utoipa::path(
        get,
        path = "/classes",
        tag = "Classes",
        summary = "List all classes",
        description = "Retrieve a list of all available classes in the knowledge base.",
        responses(
            (status = 200, description = "List of classes", body = [Class]),
            (status = 401, description = "Missing or invalid JWT token")
        )
    )]
async fn get_classes(State(state): State<AppState>) -> impl IntoResponse {
    trace!("Handling request to list all classes");
    match state.coco.get_classes().await {
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
            (status = 401, description = "Missing or invalid JWT token"),
            (status = 404, description = "Class not found")
        )
    )]
async fn get_class(State(state): State<AppState>, Path(name): Path<String>) -> impl IntoResponse {
    trace!("Handling request to get class with name: {}", name);
    match state.coco.get_class(name.clone()).await {
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
            (status = 401, description = "Missing or invalid JWT token"),
            (status = 403, description = "Forbidden - only admin users can create new classes"),
            (status = 409, description = "Class already exists"),
            (status = 500, description = "Failed to create class")
        )
    )]
async fn create_class(State(state): State<AppState>, Extension(user): Extension<CurrentUser>, Json(class): Json<Class>) -> impl IntoResponse {
    trace!("Handling request to create class with name: {}", class.name);
    if user.role != Role::Admin {
        return (StatusCode::FORBIDDEN, "Only admin users can create new classes").into_response();
    }
    match state.coco.create_class(class).await {
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
            (status = 200, description = "List of rules", body = [String]),
            (status = 401, description = "Missing or invalid JWT token")
        )
    )]
async fn get_rules(State(state): State<AppState>) -> impl IntoResponse {
    trace!("Handling request to list all rules");
    match state.coco.get_rules().await {
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
            (status = 401, description = "Missing or invalid JWT token"),
            (status = 404, description = "Rule not found")
        )
    )]
async fn get_rule(State(state): State<AppState>, Path(name): Path<String>) -> impl IntoResponse {
    trace!("Handling request to get rule with name: {}", name);
    match state.coco.get_rule(name.clone()).await {
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
            (status = 401, description = "Missing or invalid JWT token"),
            (status = 403, description = "Forbidden - only admin users can create new rules"),
            (status = 409, description = "Rule already exists"),
            (status = 500, description = "Failed to create rule")
        )
    )]
async fn create_rule(State(state): State<AppState>, Extension(user): Extension<CurrentUser>, Json(rule): Json<Rule>) -> impl IntoResponse {
    trace!("Handling request to create rule with name: {}", rule.name);
    if user.role != Role::Admin {
        return (StatusCode::FORBIDDEN, "Only admin users can create new rules").into_response();
    }
    match state.coco.create_rule(rule).await {
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
            (status = 200, description = "List of objects", body = [OpenApiObject]),
            (status = 401, description = "Missing or invalid JWT token")
        )
    )]
async fn get_objects(State(state): State<AppState>, Extension(user): Extension<CurrentUser>, Query(filter): Query<ObjectFilter>) -> impl IntoResponse {
    trace!("Handling request to list all objects with filter: {:?}", filter);
    match state.coco.get_objects().await {
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
                    if !user.has_read_access(o.id.as_ref().map(|id| id.as_str()).unwrap_or("")) {
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
            (status = 401, description = "Missing or invalid JWT token"),
            (status = 403, description = "Forbidden - you do not have permission to access this object"),
            (status = 404, description = "Object not found")
        )
    )]
async fn get_object(State(state): State<AppState>, Extension(user): Extension<CurrentUser>, Path(id): Path<String>) -> impl IntoResponse {
    trace!("Handling request to get object with ID: {}", id);
    if !user.has_read_access(&id) {
        return (StatusCode::FORBIDDEN, "You do not have permission to access this object").into_response();
    }
    match state.coco.get_object(id.clone()).await {
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
            (status = 401, description = "Missing or invalid JWT token"),
            (status = 403, description = "Forbidden - only admin users can create new objects"),
            (status = 404, description = "Class not found for object"),
            (status = 409, description = "Object already exists"),
            (status = 500, description = "Failed to create object")
        )
    )]
async fn create_object(State(state): State<AppState>, Extension(user): Extension<CurrentUser>, Json(object): Json<JsonValue>) -> impl IntoResponse {
    trace!("Handling request to create object: {:?}", object);
    if user.role != Role::Admin {
        return (StatusCode::FORBIDDEN, "Only admin users can create new objects").into_response();
    }
    match object_from_json(state.coco.clone(), object).await {
        Ok(new_object) => match state.coco.create_object(new_object).await {
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
            (status = 401, description = "Missing or invalid JWT token"),
            (status = 403, description = "Forbidden - you do not have permission to modify this object"),
            (status = 404, description = "Object not found"),
            (status = 500, description = "Failed to update object properties")
        )
    )]
async fn set_properties(State(state): State<AppState>, Extension(user): Extension<CurrentUser>, Path(object_id): Path<String>, Json(properties): Json<JsonValue>) -> impl IntoResponse {
    trace!("Handling request to set properties for object with ID: {}. New properties: {:?}", object_id, properties);
    if !user.has_write_access(&object_id) {
        return (StatusCode::FORBIDDEN, "You do not have permission to modify this object").into_response();
    }
    match state.coco.get_object_classes(object_id.clone()).await {
        Ok(classes) => match properties_from_json(state.coco.clone(), classes, properties).await {
            Ok(properties) => match state.coco.set_properties(object_id.clone(), properties).await {
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
            (status = 401, description = "Missing or invalid JWT token"),
            (status = 403, description = "Forbidden - you do not have permission to modify this object"),
            (status = 404, description = "Object not found"),
            (status = 500, description = "Failed to add data to object")
        )
    )]
async fn add_data(State(state): State<AppState>, Extension(user): Extension<CurrentUser>, Path(object_id): Path<String>, Query(date_time): Query<DateQuery>, Json(values): Json<JsonValue>) -> impl IntoResponse {
    trace!("Handling request to add data to object with ID: {}. Values: {:?}, Timestamp: {:?}", object_id, values, date_time);
    if !user.has_write_access(&object_id) {
        return (StatusCode::FORBIDDEN, "You do not have permission to modify this object").into_response();
    }
    let timestamp = date_time.time.unwrap_or_else(Utc::now);
    match state.coco.get_object_classes(object_id.clone()).await {
        Ok(classes) => match values_from_json(state.coco.clone(), classes, values).await {
            Ok(values) => match state.coco.add_values(object_id.clone(), values, timestamp).await {
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
            (status = 401, description = "Missing or invalid JWT token"),
            (status = 403, description = "Forbidden - you do not have permission to access this object's data"),
            (status = 404, description = "Object not found"),
            (status = 500, description = "Failed to retrieve object data")
        )
    )]
async fn get_data(State(state): State<AppState>, Extension(user): Extension<CurrentUser>, Path(object_id): Path<String>, Query(filter): Query<DataFilter>) -> impl IntoResponse {
    trace!("Handling request to get data for object with ID: {}. Time filter: {:?}", object_id, filter);
    if !user.has_read_access(&object_id) {
        return (StatusCode::FORBIDDEN, "You do not have permission to access this object's data").into_response();
    }
    match state.coco.get_values(object_id.clone(), filter.start, filter.end).await {
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

#[derive(Deserialize)]
struct WsQuery {
    token: String,
}

#[utoipa::path(
        get,
        path = "/ws",
        tag = "System",
        summary = "WebSocket connection",
        description = "Establish a WebSocket connection for real-time updates.",
        params(
            ("token" = String, Query, description = "JWT token for authentication")
        ),
        responses(
            (status = 101, description = "WebSocket connection established"),
            (status = 401, description = "Missing or invalid JWT token"),
        )
    )]
async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>, Query(query): Query<WsQuery>) -> Result<impl IntoResponse, StatusCode> {
    let claims = verify_jwt(&query.token, state.users_db.secret()).map_err(|_| StatusCode::UNAUTHORIZED)?;
    if claims.token_type != TokenType::Access {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let user = state.users_db.get_user_by_username(&claims.sub).await.map_err(|_| StatusCode::UNAUTHORIZED)?;
    let current_user = CurrentUser { role: user.role, read_access: user.read_access, write_access: user.write_access };
    Ok(ws.on_upgrade(move |socket| async move { handle_socket(socket, state, current_user).await }))
}

async fn handle_socket(mut socket: WebSocket, state: AppState, user: CurrentUser) {
    trace!("WebSocket connection established");

    let init_msg = match async {
        let classes_map: HashMap<String, serde_json::Value> = state
            .coco
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

        let rules_map: HashMap<String, serde_json::Value> = state
            .coco
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

        let objects_map: HashMap<String, serde_json::Value> = state
            .coco
            .get_objects()
            .await?
            .into_iter()
            .filter(|o| user.has_read_access(o.id.as_ref().map(|id| id.as_str()).unwrap_or("")))
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

    let mut rx = state.coco.event_tx.subscribe();
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
                    CoCoEvent::ClassCreated(class_name) => match state.coco.get_class(class_name).await {
                        Ok(Some(class)) => {
                            let mut update_msg = serde_json::to_value(class).unwrap();
                            update_msg["msg_type"] = serde_json::json!("class-created");
                            socket.send(Message::Text(serde_json::to_string(&update_msg).unwrap().into())).await
                        }
                        Ok(None) => Ok(()),
                        Err(_) => Ok(()),
                    },
                    CoCoEvent::RuleCreated(rule) => match state.coco.get_rule(rule).await {
                        Ok(Some(rule)) => {
                            let mut update_msg = serde_json::to_value(rule).unwrap();
                            update_msg["msg_type"] = serde_json::json!("rule-created");
                            socket.send(Message::Text(serde_json::to_string(&update_msg).unwrap().into())).await
                        }
                        Ok(None) => Ok(()),
                        Err(_) => Ok(()),
                    },
                    CoCoEvent::ObjectCreated(object_id) => {
                        if user.has_read_access(&object_id) {
                            match state.coco.get_object(object_id).await {
                                Ok(Some(object)) => {
                                    let mut update_msg = serde_json::to_value(object).unwrap();
                                    update_msg["msg_type"] = serde_json::json!("object-created");
                                    socket.send(Message::Text(serde_json::to_string(&update_msg).unwrap().into())).await
                                }
                                Ok(None) => Ok(()),
                                Err(_) => Ok(()),
                            }
                        } else {
                            Ok(())
                        }
                    }
                    CoCoEvent::AddedClass(object_id, class_name) => {
                        let update_msg = serde_json::json!({
                            "msg_type": "added-class",
                            "object_id": object_id,
                            "class_name": class_name
                        });
                        socket.send(Message::Text(serde_json::to_string(&update_msg).unwrap().into())).await
                    }
                    CoCoEvent::UpdatedProperties(object_id, properties) => {
                        if user.has_read_access(&object_id) {
                            let update_msg = serde_json::json!({
                                "msg_type": "updated-properties",
                                "object_id": object_id,
                                "properties": properties
                            });
                            socket.send(Message::Text(serde_json::to_string(&update_msg).unwrap().into())).await
                        } else {
                            Ok(())
                        }
                    }
                    CoCoEvent::AddedValues(object_id, values, date_time) => {
                        if user.has_read_access(&object_id) {
                            let update_msg = serde_json::json!({
                                "msg_type": "added-values",
                                "object_id": object_id,
                                "values": values,
                                "date_time": date_time
                            });
                            socket.send(Message::Text(serde_json::to_string(&update_msg).unwrap().into())).await
                        } else {
                            Ok(())
                        }
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

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme("bearerAuth", SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Bearer).bearer_format("JWT").build()));
    }
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
    modifiers(&SecurityAddon),
    tags(
        (name = "Authentication", description = "Endpoints for user registration and login"),
        (name = "Classes", description = "Operations related to knowledge base classes"),
        (name = "Objects", description = "Operations related to knowledge base objects"),
        (name = "Rules", description = "Operations related to knowledge base rules"),
        (name = "System", description = "System and utility endpoints")
    )
)]
pub struct ApiDoc;
