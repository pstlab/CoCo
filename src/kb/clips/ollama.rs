use crate::{
    kb::{CLIPSKnowledgeBase, KnowledgeBaseError},
    model::Value,
};
use clips::{ClipsValue, Type};
use reqwest::Client;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::error;

pub async fn setup_ollama(kb: &CLIPSKnowledgeBase) -> Result<(), KnowledgeBaseError> {
    let host = std::env::var("OLLAMA_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = std::env::var("OLLAMA_PORT").unwrap_or_else(|_| "11434".to_string()).parse::<u16>().unwrap_or(11434);
    let model = std::env::var("OLLAMA_MODEL").unwrap_or_else(|_| "llama3".to_string());
    add_ollama(kb, host, port, model).await
}

pub async fn add_ollama(kb: &CLIPSKnowledgeBase, host: String, port: u16, model: String) -> Result<(), KnowledgeBaseError> {
    let url = format!("http://{}:{}/api/chat", host, port);
    let client = Client::new();

    kb.build("(deftemplate llm-result (slot item_id (type SYMBOL)) (slot result (type STRING)))").await?;
    let (tx, rx) = mpsc::channel::<(String, String)>(100);

    let kb_clone = kb.clone();
    tokio::spawn(async move {
        let mut rx = rx;
        let mut llm_result = HashMap::new();
        while let Some((item_id, result)) = rx.recv().await {
            if let Some(fact) = llm_result.get(&item_id) {
                match kb_clone.modify_fact(*fact, HashMap::from([("item_id".to_string(), Value::Symbol(item_id.clone())), ("result".to_string(), Value::String(result))])).await {
                    Ok(_) => (),
                    Err(e) => error!("Failed to modify fact for item_id {}: {}", item_id, e),
                }
            } else {
                match kb_clone.assert_fact("llm-result", HashMap::from([("item_id".to_string(), Value::Symbol(item_id.clone())), ("result".to_string(), Value::String(result))])).await {
                    Ok(fact) => {
                        llm_result.insert(item_id, fact);
                    }
                    Err(e) => error!("Failed to assert fact for item_id {}: {}", item_id, e),
                }
            }
        }
    });

    kb.add_udf(
        "prompt",
        None,
        2,
        2,
        vec![Type(Type::SYMBOL), Type(Type::STRING)],
        Box::new(move |_env, ctx: &mut clips::UDFContext| {
            let object_id_val = ctx.get_next_argument(Type(Type::SYMBOL)).expect("Failed to get object ID argument for prompt UDF");
            let object_id = match object_id_val {
                ClipsValue::Symbol(s) => s.to_string(),
                _ => panic!("Expected symbol for object ID argument in prompt UDF"),
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
                let body = serde_json::json!({
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "stream": false
                });

                let res_content = match client.post(&url).json(&body).send().await {
                    Ok(response) => match response.json::<serde_json::Value>().await {
                        Ok(json) => json["message"]["content"].as_str().map(|c| c.to_string()).unwrap_or_else(|| "Parse error".to_string()),
                        Err(_) => "Parse error".to_string(),
                    },
                    Err(_) => "Connection error".to_string(),
                };

                let _ = tx.send((object_id, res_content)).await;
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
