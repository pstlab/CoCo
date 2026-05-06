use crate::{CoCo, CoCoModule, db::Database, kb::clips::CLIPSKnowledgeBase, model::CoCoError};
use async_trait::async_trait;
use clips::{ClipsValue, Type};
use futures_util::StreamExt;
use reqwest::{Client, Response};
use serde::Deserialize;
use tracing::{error, info, trace};

pub struct OllamaModule {
    model: String,
    url: String,
    client: Client,
}

impl OllamaModule {
    pub fn new(host: String, port: u16, model: String) -> Self {
        let url = format!("http://{}:{}/api/chat", host, port);
        info!("Initializing OllamaModule with model '{}' at {}", model, url);
        let client = Client::new();
        Self { model, url, client }
    }
}

impl Default for OllamaModule {
    fn default() -> Self {
        let host = std::env::var("OLLAMA_HOST").unwrap_or_else(|_| "localhost".to_string());
        let port = std::env::var("OLLAMA_PORT").unwrap_or_else(|_| "11434".to_string()).parse::<u16>().unwrap_or(11434);
        let model = std::env::var("OLLAMA_MODEL").unwrap_or_else(|_| "llama3".to_string());
        Self::new(host, port, model)
    }
}

#[async_trait]
impl<DB: Database> CoCoModule<DB, CLIPSKnowledgeBase> for OllamaModule {
    async fn init(&self, _db: DB, kb: CLIPSKnowledgeBase, _coco: CoCo) -> Result<(), CoCoError> {
        let client = self.client.clone();
        let url = self.url.clone();
        let model = self.model.clone();

        kb.add_udf(
            "prompt",
            None,
            2,
            2,
            vec![Type(Type::SYMBOL), Type(Type::STRING)],
            Box::new(move |_env, ctx: &mut UDFContext| {
                let object_id = match ctx.get_next_argument(Type(Type::SYMBOL)).expect("Failed to get object ID argument for prompt UDF") {
                    ClipsValue::Symbol(s) => s.to_string(),
                    _ => panic!("Expected symbol for object ID argument in prompt UDF"),
                };

                let prompt = match ctx.get_next_argument(Type(Type::STRING)).expect("Failed to get prompt argument for prompt UDF") {
                    ClipsValue::String(s) => s.to_string(),
                    _ => panic!("Expected string for prompt argument in prompt UDF"),
                };

                let client = client.clone();
                let url = url.clone();
                let model = model.clone();
                tokio::spawn(async move {
                    trace!("Sending prompt to Ollama for object_id {}: {}", object_id, prompt);
                    let body = serde_json::json!({
                        "model": model,
                        "messages": [{"role": "user", "content": prompt}],
                        "stream": true
                    });

                    match client.post(&url).json(&body).send().await {
                        Ok(response) => {
                            parse_response(response).await;
                        }
                        Err(_) => {
                            error!("Failed to send request to Ollama for object_id {}: {}", object_id, url);
                        }
                    };
                });

                ClipsValue::Void()
            }),
        )
        .await
        .map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to add prompt UDF: {}", e)))?;

        Ok(())
    }
}

#[derive(Deserialize, Debug)]
struct OllamaResponse {
    response: String,
    done: bool,
}

async fn parse_response(response: Response) {
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(c) => c,
            Err(e) => {
                error!("Error reading stream from Ollama: {}", e);
                break;
            }
        };
        let chunk_str = match std::str::from_utf8(&chunk) {
            Ok(s) => s,
            Err(e) => {
                error!("Error converting chunk to string: {}", e);
                continue;
            }
        };
        for line in chunk_str.lines() {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<OllamaResponse>(line) {
                Ok(ollama_response) => {
                    trace!("{}", ollama_response.response);
                    if ollama_response.done {
                        trace!("Ollama response complete");
                    }
                }
                Err(e) => {
                    error!("Error parsing Ollama response: {}", e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tracing::{Level, subscriber};

    use super::*;

    #[tokio::test]
    async fn test_parse_response() {
        let subscriber = tracing_subscriber::fmt().with_max_level(Level::TRACE).finish();
        subscriber::set_global_default(subscriber).expect("Failed to set global default subscriber");

        let host = std::env::var("OLLAMA_HOST").unwrap_or_else(|_| "localhost".to_string());
        let port = std::env::var("OLLAMA_PORT").unwrap_or_else(|_| "11434".to_string()).parse::<u16>().unwrap_or(11434);
        let model = std::env::var("OLLAMA_MODEL").unwrap_or_else(|_| "llama3".to_string());
        let url = format!("http://{}:{}/api/generate", host, port);
        let client = Client::new();
        let body = serde_json::json!({
            "model": model,
            "prompt": "Hello, Ollama!"
        });

        match client.post(&url).json(&body).send().await {
            Ok(response) => {
                parse_response(response).await;
            }
            Err(_) => {
                error!("Failed to send request to Ollama for test: {}", url);
            }
        };
    }
}
