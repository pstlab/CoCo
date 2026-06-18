use crate::{CoCo, CoCoModule, db::Database, kb::clips::CLIPSKnowledgeBase, model::CoCoError};
use async_trait::async_trait;
use clips::{ClipsValue, Type, UDFContext};
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
        let url = format!("http://{}:{}/api/generate", host, port);
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

        let parse_kb = kb.clone();
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
                let parse_kb = parse_kb.clone();
                tokio::spawn(async move {
                    trace!("Sending prompt to Ollama for object_id {}: {}", object_id, prompt);
                    let body = serde_json::json!({
                        "model": model,
                        "prompt": prompt
                    });

                    match client.post(&url).json(&body).send().await {
                        Ok(response) => {
                            parse_response(parse_kb, response).await;
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

fn parse_ollama_line(line: &str, full_text: &mut String) {
    match serde_json::from_str::<OllamaResponse>(line) {
        Ok(ollama_response) => {
            trace!("{}", ollama_response.response);
            full_text.push_str(&ollama_response.response);
            if ollama_response.done {
                trace!("Ollama response complete");
            }
        }
        Err(e) => {
            error!("Error parsing Ollama response: {}", e);
        }
    }
}

async fn parse_response(_kb: CLIPSKnowledgeBase, response: Response) {
    let mut stream = response.bytes_stream();
    let mut full_text = String::new();
    let mut pending = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(c) => c,
            Err(e) => {
                error!("Error reading stream from Ollama: {}", e);
                break;
            }
        };
        pending.extend_from_slice(&chunk);

        while let Some(newline_idx) = pending.iter().position(|&b| b == b'\n') {
            let mut line_bytes = pending.drain(..=newline_idx).collect::<Vec<u8>>();
            if line_bytes.last() == Some(&b'\n') {
                line_bytes.pop();
            }
            if line_bytes.last() == Some(&b'\r') {
                line_bytes.pop();
            }

            if line_bytes.is_empty() {
                continue;
            }

            match std::str::from_utf8(&line_bytes) {
                Ok(line_str) => parse_ollama_line(line_str, &mut full_text),
                Err(e) => error!("Invalid UTF-8 sequence in line: {}", e),
            }
        }
    }

    if !pending.is_empty() {
        if let Ok(line_str) = std::str::from_utf8(&pending) {
            let trimmed = line_str.trim_end_matches(['\r', '\n']);
            if !trimmed.is_empty() {
                parse_ollama_line(trimmed, &mut full_text);
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

        let (kb, _) = CLIPSKnowledgeBase::new();
        match client.post(&url).json(&body).send().await {
            Ok(response) => {
                parse_response(kb, response).await;
            }
            Err(_) => {
                error!("Failed to send request to Ollama for test: {}", url);
            }
        };
    }
}
