use crate::{
    CoCoModule,
    db::{Database, mongodb::MongoDB},
    kb::clips::CLIPSKnowledgeBase,
    model::CoCoError,
    server::{
        auth::{CurrentUser, auth_middleware},
        auth_db::UsersDB,
    },
};
use async_trait::async_trait;
use axum::{
    Extension, Router,
    extract::{Path, State},
    middleware::from_fn_with_state,
    response::IntoResponse,
    routing::post,
};
use clips::{ClipsValue, Type};
use mongodb::bson::{Document, doc, oid::ObjectId};
use reqwest::{Client, StatusCode};
use serde_json::json;
use std::{env, fmt, sync::Arc};
use tracing::{error, trace, warn};
use yup_oauth2::{ServiceAccountAuthenticator, read_service_account_key};

pub struct FCMModule {
    project_id: String,
}

impl FCMModule {
    pub fn new(project_id: String) -> Self {
        Self { project_id }
    }
}

impl Default for FCMModule {
    fn default() -> Self {
        let project_id = env::var("FCM_PROJECT_ID").unwrap_or_else(|_| "your-project-id".to_string());
        Self::new(project_id)
    }
}

#[async_trait]
impl CoCoModule<MongoDB, CLIPSKnowledgeBase> for FCMModule {
    async fn init(&self, db: MongoDB, kb: CLIPSKnowledgeBase, _coco: crate::CoCo) -> Result<(), CoCoError> {
        let url = format!("https://fcm.googleapis.com/v1/projects/{}/messages:send", self.project_id);
        let client = Client::new();
        let db_for_udf = db.clone();

        let key_path = env::var("GOOGLE_APPLICATION_CREDENTIALS").map_err(|_| CoCoError::ConfigurationError("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set".to_string()))?;
        let key = read_service_account_key(&key_path).await.map_err(|e| CoCoError::ConfigurationError(format!("Failed to read service account key from {}: {}", key_path, e)))?;

        let auth = ServiceAccountAuthenticator::builder(key).build().await.map_err(|e| CoCoError::ConfigurationError(format!("Failed to create authenticator: {}", e)))?;

        let shared_auth = Arc::new(auth);
        kb.add_udf(
            "send-message",
            vec![Type(Type::VOID)],
            3,
            3,
            vec![vec![Type(Type::SYMBOL)], vec![Type(Type::STRING)], vec![Type(Type::STRING)]],
            Box::new(move |_env, ctx| {
                let object_id = match ctx.get_next_argument(Type(Type::SYMBOL)) {
                    Some(ClipsValue::Symbol(s)) => s,
                    _ => {
                        error!("Expected symbol for object ID argument in send-message UDF");
                        return ClipsValue::Void();
                    }
                };
                let title = match ctx.get_next_argument(Type(Type::STRING)) {
                    Some(ClipsValue::String(s)) => s,
                    _ => {
                        error!("Expected string for title argument in send-message UDF");
                        return ClipsValue::Void();
                    }
                };
                let message = match ctx.get_next_argument(Type(Type::STRING)) {
                    Some(ClipsValue::String(s)) => s,
                    _ => {
                        error!("Expected string for message argument in send-message UDF");
                        return ClipsValue::Void();
                    }
                };

                let db = db_for_udf.clone();
                let client = client.clone();
                let url = url.clone();
                let auth_for_task = shared_auth.clone();
                tokio::spawn(async move {
                    let tokens = match db.get_tokens(&object_id).await {
                        Ok(tokens) => tokens,
                        Err(e) => {
                            warn!("Failed to load FCM tokens for object_id={}: {}", object_id, e);
                            return;
                        }
                    };

                    if tokens.is_empty() {
                        trace!("No FCM tokens found for object_id={}", object_id);
                        return;
                    }

                    let scopes = &["https://www.googleapis.com/auth/firebase.messaging"];
                    let access_token = match auth_for_task.token(scopes).await {
                        Ok(token) => match token.token() {
                            Some(t) => t.to_owned(),
                            None => {
                                warn!("Token payload is missing access token");
                                return;
                            }
                        },
                        Err(e) => {
                            warn!("Failed to get FCM access token: {}", e);
                            return;
                        }
                    };

                    for token in tokens {
                        if let Err(e) = send_message(&client, &url, &access_token, &token, &title, &message).await {
                            if e.is_unregistered() {
                                warn!("FCM reports an unregistered token for object_id={}; removing it: {}", object_id, e);
                                if let Err(remove_err) = db.remove_token(&object_id, &token).await {
                                    warn!("Failed removing stale token for object_id={}: {}", object_id, remove_err);
                                }
                            } else {
                                warn!("FCM send failed for object_id={}; keeping token: {}", object_id, e);
                            }
                        }
                    }
                });

                ClipsValue::Void()
            }),
        )
        .await
        .map_err(|e| CoCoError::ConfigurationError(format!("Failed to add send-message UDF to knowledge base: {}", e)))?;

        Ok(())
    }
}

#[async_trait]
trait FCMTokenManager {
    async fn add_token(&self, object_id: &str, token: &str) -> Result<(), String>;
    async fn remove_token(&self, object_id: &str, token: &str) -> Result<(), String>;
    async fn get_tokens(&self, object_id: &str) -> Result<Vec<String>, String>;
}

#[async_trait]
impl FCMTokenManager for MongoDB {
    async fn add_token(&self, object_id: &str, token: &str) -> Result<(), String> {
        let oid = ObjectId::parse_str(object_id).map_err(|e| e.to_string())?;
        let collection = self.client.database(self.name()).collection::<Document>("fcm_tokens");
        collection.update_one(doc! { "_id": oid }, doc! { "$addToSet": { "tokens": token } }).upsert(true).await.map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn remove_token(&self, object_id: &str, token: &str) -> Result<(), String> {
        let oid = ObjectId::parse_str(object_id).map_err(|e| e.to_string())?;
        let collection = self.client.database(self.name()).collection::<Document>("fcm_tokens");
        collection.update_one(doc! { "_id": oid }, doc! { "$pull": { "tokens": token } }).await.map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn get_tokens(&self, object_id: &str) -> Result<Vec<String>, String> {
        let oid = ObjectId::parse_str(object_id).map_err(|e| e.to_string())?;
        let collection = self.client.database(self.name()).collection::<Document>("fcm_tokens");
        let doc = collection.find_one(doc! { "_id": oid }).await.map_err(|e| e.to_string())?;

        if let Some(doc) = doc { if let Ok(tokens) = doc.get_array("tokens") { Ok(tokens.iter().filter_map(|v| v.as_str()).map(|s| s.to_string()).collect()) } else { Ok(vec![]) } } else { Ok(vec![]) }
    }
}

pub fn fcm_router(db: MongoDB, users_db: UsersDB) -> Router {
    Router::new().route("/add_token/{id}", post(add_token)).route_layer(from_fn_with_state(users_db, auth_middleware)).with_state(db)
}

async fn add_token(State(db): State<MongoDB>, Extension(user): Extension<CurrentUser>, Path(id): Path<String>, token: String) -> impl IntoResponse {
    trace!("Adding FCM token for ID: {}", id);
    if !user.has_read_access(&id) {
        return (StatusCode::FORBIDDEN, "You do not have permission to receive notifications for this object").into_response();
    }

    let token = token.trim();
    if token.is_empty() {
        return (StatusCode::BAD_REQUEST, "Token must not be empty".to_string()).into_response();
    }

    let object_id = match ObjectId::parse_str(&id) {
        Ok(oid) => oid,
        Err(_) => return (StatusCode::BAD_REQUEST, "ID must be a valid MongoDB ObjectId".to_string()).into_response(),
    };

    match db.add_token(&object_id.to_hex(), token).await {
        Ok(_) => (StatusCode::OK, "Token added successfully".to_string()).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to add token: {}", e)).into_response(),
    }
}

struct FCMError {
    message: String,
    unregistered: bool,
}

impl FCMError {
    fn is_unregistered(&self) -> bool {
        self.unregistered
    }
}

impl fmt::Display for FCMError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

async fn send_message(client: &Client, url: &str, access_token: &str, token: &str, title: &str, message: &str) -> Result<(), FCMError> {
    let payload = json!({
        "message": {
            "token": token,
            "notification": {
                "title": title,
                "body": message,
            }
        }
    });

    let response = client.post(url).bearer_auth(access_token).json(&payload).send().await.map_err(|e| FCMError { message: e.to_string(), unregistered: false })?;

    if response.status().is_success() {
        return Ok(());
    }

    let status = response.status();
    let body = response.text().await.unwrap_or_else(|_| "<empty response body>".to_string());
    Err(FCMError {
        message: format!("FCM returned status {}", status),
        unregistered: status == StatusCode::NOT_FOUND && body.contains("\"UNREGISTERED\""),
    })
}
