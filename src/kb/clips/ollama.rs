use crate::{
    kb::{CLIPSKnowledgeBase, KnowledgeBase, KnowledgeBaseError},
    model::Value,
};
use chrono::Utc;
use clips::{ClipsValue, Type};
use reqwest::Client;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{error, info, trace};

enum Tool {
    AddValues { object_id: String, values: HashMap<String, Value> },
}

pub async fn setup_ollama(kb: &CLIPSKnowledgeBase) -> Result<(), KnowledgeBaseError> {
    let host = std::env::var("OLLAMA_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = std::env::var("OLLAMA_PORT").unwrap_or_else(|_| "11434".to_string()).parse::<u16>().unwrap_or(11434);
    let model = std::env::var("OLLAMA_MODEL").unwrap_or_else(|_| "llama3".to_string());
    add_ollama(kb, host, port, model).await
}

pub async fn add_ollama(kb: &CLIPSKnowledgeBase, host: String, port: u16, model: String) -> Result<(), KnowledgeBaseError> {
    info!("Setting up Ollama integration with model '{}' at {}:{}", model, host, port);
    let url = format!("http://{}:{}/api/chat", host, port);
    let client = Client::new();

    let (tx, rx) = mpsc::channel::<Tool>(100);

    let kb_clone = kb.clone();
    tokio::spawn(async move {
        let mut rx = rx;
        while let Some(tool) = rx.recv().await {
            match tool {
                Tool::AddValues { object_id, values } => {
                    if let Err(e) = kb_clone.add_values(object_id.clone(), values, Utc::now()).await {
                        error!("Failed to add values for object_id {}: {}", object_id, e);
                    }
                }
            }
        }
    });

    kb.add_udf(
        "prompt",
        None,
        3,
        3,
        vec![Type(Type::SYMBOL), Type(Type::SYMBOL), Type(Type::STRING)],
        Box::new(move |_env, ctx: &mut clips::UDFContext| {
            let object_id_val = ctx.get_next_argument(Type(Type::SYMBOL)).expect("Failed to get object ID argument for prompt UDF");
            let object_id = match object_id_val {
                ClipsValue::Symbol(s) => s.to_string(),
                _ => panic!("Expected symbol for object ID argument in prompt UDF"),
            };

            let content_id_val = ctx.get_next_argument(Type(Type::SYMBOL)).expect("Failed to get content ID argument for prompt UDF");
            let content_id = match content_id_val {
                ClipsValue::Symbol(s) => s.to_string(),
                _ => panic!("Expected symbol for content ID argument in prompt UDF"),
            };

            let prompt_val = ctx.get_next_argument(Type(Type::STRING)).expect("Failed to get prompt argument for prompt UDF");
            let prompt = match prompt_val {
                ClipsValue::String(s) => s.to_string(),
                _ => panic!("Expected string for prompt argument in prompt UDF"),
            };

            let client = client.clone();
            let url = url.clone();
            let model = model.clone();
            let tx = tx.clone();

            tokio::spawn(async move {
                trace!("Sending prompt to Ollama for object_id {}: {}", object_id, prompt);
                let body = serde_json::json!({
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "stream": true
                });

                match client.post(&url).json(&body).send().await {
                    Ok(response) => match response.json::<serde_json::Value>().await {
                        Ok(json) => {
                            let mut vals = HashMap::new();
                            if let Some(content) = json["content"].as_str()
                                && !content.is_empty()
                            {
                                vals.insert(content_id, Value::String(content.to_string()));
                            }
                            let _ = tx.send(Tool::AddValues { object_id, values: vals }).await;
                        }
                        Err(_) => {
                            error!("Failed to parse response from Ollama for object_id {}: {}", object_id, url);
                        }
                    },
                    Err(_) => {
                        error!("Failed to send request to Ollama for object_id {}: {}", object_id, url);
                    }
                };
            });

            ClipsValue::Void()
        }),
    )
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::kb::setup_clips;

    use super::*;

    #[tokio::test]
    async fn test_add_ollama() {
        let kb = setup_clips().unwrap_or_else(|e| {
            error!("Failed to set up knowledge base: {}", e);
            std::process::exit(1);
        });

        setup_ollama(&kb).await.unwrap_or_else(|e| {
            error!("Failed to set up Ollama integration: {}", e);
            std::process::exit(1);
        });
    }
}
