use crate::{
    db::{Database, mongodb::MongoDB},
    kb::clips::CLIPSKnowledgeBase,
    model::CoCoError,
};
use axum::{
    Router,
    extract::{Path, State},
    response::IntoResponse,
    routing::post,
};
use clips::{ClipsValue, Type};
use mongodb::bson::{Document, doc, oid::ObjectId};
use reqwest::{Client, StatusCode};
use serde_json::json;
use tracing::{trace, warn};
use yup_oauth2::{ServiceAccountAuthenticator, read_service_account_key};

pub async fn setup_fcm(db: MongoDB, kb: &CLIPSKnowledgeBase) -> Result<(), CoCoError> {
    let project_id = std::env::var("FCM_PROJECT_ID").map_err(|_| CoCoError::ConfigurationError("Missing FCM_PROJECT_ID environment variable".to_string()))?;

    add_fcm(db, kb, project_id).await
}

pub async fn add_fcm(db: MongoDB, kb: &CLIPSKnowledgeBase, project_id: String) -> Result<(), CoCoError> {
    let url = format!("https://fcm.googleapis.com/v1/projects/{}/messages:send", project_id);
    let client = Client::new();
    let db_for_udf = db.clone();

    kb.add_udf(
        "send-message",
        None,
        3,
        3,
        vec![Type(Type::SYMBOL), Type(Type::STRING), Type(Type::STRING)],
        Box::new(move |_env, ctx| {
            let object_id = ctx.get_next_argument(Type(Type::SYMBOL)).expect("Failed to get object ID argument for send-message UDF");
            let object_id = if let ClipsValue::Symbol(s) = object_id { s } else { panic!("Expected symbol for object ID argument in send-message UDF") };
            let title = ctx.get_next_argument(Type(Type::STRING)).expect("Failed to get title argument for send-message UDF");
            let title = if let ClipsValue::String(s) = title { s } else { panic!("Expected string for title argument in send-message UDF") };
            let message = ctx.get_next_argument(Type(Type::STRING)).expect("Failed to get message argument for send-message UDF");
            let message = if let ClipsValue::String(s) = message { s } else { panic!("Expected string for message argument in send-message UDF") };

            let db = db_for_udf.clone();
            let client = client.clone();
            let url = url.clone();
            tokio::spawn(async move {
                let tokens = match get_fcm_tokens(&db, &object_id).await {
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

                let access_token = match get_token().await {
                    Ok(token) => token,
                    Err(e) => {
                        warn!("Failed to get FCM access token: {}", e);
                        return;
                    }
                };

                for token in tokens {
                    if let Err(e) = send_message(&client, &url, &access_token, &token, &title, &message).await {
                        warn!("FCM send failed for object_id={}, token={} (removing token): {}", object_id, token, e);
                        if let Err(remove_err) = remove_fcm_token(&db, &object_id, &token).await {
                            warn!("Failed removing stale token for object_id={}, token={}: {}", object_id, token, remove_err);
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

pub fn fcm_router(db: MongoDB) -> Router {
    Router::new().route("/add_token/{id}", post(add_token)).with_state(db)
}

async fn get_token() -> Result<String, String> {
    let key_path = std::env::var("SERVICE_ACCOUNT_FILE").unwrap_or_else(|_| "service-account.json".to_string());
    let key = read_service_account_key(key_path).await.map_err(|e| format!("Failed to read service account key: {}", e))?;
    let auth = ServiceAccountAuthenticator::builder(key).build().await.map_err(|e| format!("Failed to create authenticator: {}", e))?;
    let scopes = &["https://www.googleapis.com/auth/firebase.messaging"];
    let token = auth.token(scopes).await.map_err(|e| format!("Failed to get token: {}", e))?;
    token.token().map(|t| t.to_owned()).ok_or_else(|| "Token payload is missing access token".to_string())
}

async fn add_token(State(db): State<MongoDB>, Path(id): Path<String>, token: String) -> impl IntoResponse {
    trace!("Adding FCM token for ID: {}", id);
    let token = token.trim();
    if token.is_empty() {
        return (StatusCode::BAD_REQUEST, "Token must not be empty".to_string()).into_response();
    }

    let object_id = match ObjectId::parse_str(&id) {
        Ok(oid) => oid,
        Err(_) => return (StatusCode::BAD_REQUEST, "ID must be a valid MongoDB ObjectId".to_string()).into_response(),
    };

    match add_fcm_token(&db, &object_id.to_hex(), token).await {
        Ok(_) => (StatusCode::OK, "Token added successfully".to_string()).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to add token: {}", e)).into_response(),
    }
}

async fn add_fcm_token(db: &MongoDB, object_id: &str, token: &str) -> Result<(), String> {
    let oid = ObjectId::parse_str(object_id).map_err(|e| e.to_string())?;
    let collection = db.client.database(db.name()).collection::<Document>("fcm_tokens");
    collection.update_one(doc! { "_id": oid }, doc! { "$addToSet": { "tokens": token } }).upsert(true).await.map_err(|e| e.to_string())?;
    Ok(())
}

async fn remove_fcm_token(db: &MongoDB, object_id: &str, token: &str) -> Result<(), String> {
    let oid = ObjectId::parse_str(object_id).map_err(|e| e.to_string())?;
    let collection = db.client.database(db.name()).collection::<Document>("fcm_tokens");
    collection.update_one(doc! { "_id": oid }, doc! { "$pull": { "tokens": token } }).await.map_err(|e| e.to_string())?;
    Ok(())
}

async fn get_fcm_tokens(db: &MongoDB, object_id: &str) -> Result<Vec<String>, String> {
    let oid = ObjectId::parse_str(object_id).map_err(|e| e.to_string())?;
    let collection = db.client.database(db.name()).collection::<Document>("fcm_tokens");
    let doc = collection.find_one(doc! { "_id": oid }).await.map_err(|e| e.to_string())?;

    if let Some(doc) = doc { if let Ok(tokens) = doc.get_array("tokens") { Ok(tokens.iter().filter_map(|v| v.as_str()).map(|s| s.to_string()).collect()) } else { Ok(vec![]) } } else { Ok(vec![]) }
}

async fn send_message(client: &Client, url: &str, access_token: &str, token: &str, title: &str, message: &str) -> Result<(), String> {
    let payload = json!({
        "message": {
            "token": token,
            "notification": {
                "title": title,
                "body": message,
            }
        }
    });

    let response = client.post(url).bearer_auth(access_token).json(&payload).send().await.map_err(|e| e.to_string())?;

    if response.status().is_success() {
        return Ok(());
    }

    let status = response.status();
    let body = response.text().await.unwrap_or_else(|_| "<empty response body>".to_string());
    Err(format!("FCM returned status {} with body {}", status, body))
}
