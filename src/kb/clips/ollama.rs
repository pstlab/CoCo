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
    SetProperties { object_id: String, properties: HashMap<String, Value> },
    AddValues { object_id: String, values: HashMap<String, Value> },
}

fn json_to_model_value(value: &serde_json::Value) -> Option<Value> {
    match value {
        serde_json::Value::Null => Some(Value::Null),
        serde_json::Value::Bool(b) => Some(Value::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(Value::Int(i))
            } else {
                n.as_f64().map(Value::Float)
            }
        }
        serde_json::Value::String(s) => Some(Value::String(s.clone())),
        serde_json::Value::Array(arr) => {
            if arr.is_empty() {
                return None;
            }

            let all_bool = arr.iter().all(serde_json::Value::is_boolean);
            if all_bool {
                let vals = arr.iter().filter_map(serde_json::Value::as_bool).collect::<Vec<_>>();
                return Some(Value::BoolArray(vals));
            }

            let all_i64 = arr.iter().all(|v| v.as_i64().is_some());
            if all_i64 {
                let vals = arr.iter().filter_map(serde_json::Value::as_i64).collect::<Vec<_>>();
                return Some(Value::IntArray(vals));
            }

            let all_number = arr.iter().all(serde_json::Value::is_number);
            if all_number {
                let vals = arr.iter().filter_map(serde_json::Value::as_f64).collect::<Vec<_>>();
                return Some(Value::FloatArray(vals));
            }

            let all_string = arr.iter().all(serde_json::Value::is_string);
            if all_string {
                let vals = arr.iter().filter_map(serde_json::Value::as_str).map(ToOwned::to_owned).collect::<Vec<_>>();
                return Some(Value::StringArray(vals));
            }

            None
        }
        serde_json::Value::Object(_) => None,
    }
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
                Tool::SetProperties { object_id, properties } => {
                    if let Err(e) = kb_clone.set_properties(object_id.clone(), properties).await {
                        error!("Failed to set properties for object_id {}: {}", object_id, e);
                    }
                }
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
                    "stream": false,
                    "tools": [
                        {
                            "name": "set_properties",
                            "description": "Set properties for an object in the knowledge base",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "properties": {"type": "object", "description": "The properties to set for the object"}
                                },
                                "required": ["object_id", "properties"]
                            }
                        },
                        {
                            "name": "add_values",
                            "description": "Add values for an object in the knowledge base",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "values": {"type": "object", "description": "The values to add for the object"}
                                },
                                "required": ["object_id", "values"]
                            }
                        }
                    ]
                });

                match client.post(&url).json(&body).send().await {
                    Ok(response) => match response.json::<serde_json::Value>().await {
                        Ok(json) => {
                            let mut vals = HashMap::new();
                            if let Some(content) = json["message"]["content"].as_str()
                                && !content.is_empty()
                            {
                                vals.insert(content_id, Value::String(content.to_string()));
                            }
                            if let Some(tools) = json["message"]["tool_calls"].as_array() {
                                for tool in tools {
                                    if let Some(tool_name) = tool["name"].as_str() {
                                        match tool_name {
                                            "set_properties" => {
                                                if let Some(properties) = tool["arguments"].as_object() {
                                                    let mut props = HashMap::new();
                                                    for (key, value) in properties {
                                                        if let Some(v) = json_to_model_value(value) {
                                                            props.insert(key.clone(), v);
                                                        }
                                                    }
                                                    let _ = tx.send(Tool::SetProperties { object_id: object_id.clone(), properties: props }).await;
                                                }
                                            }
                                            "add_values" => {
                                                if let Some(values) = tool["arguments"].as_object() {
                                                    let mut vals = HashMap::new();
                                                    for (key, value) in values {
                                                        if let Some(v) = json_to_model_value(value) {
                                                            vals.insert(key.clone(), v);
                                                        }
                                                    }
                                                    let _ = tx.send(Tool::AddValues { object_id: object_id.clone(), values: vals }).await;
                                                }
                                            }
                                            _ => {
                                                error!("Unknown tool called by Ollama for object_id {}: {}", object_id, tool_name);
                                            }
                                        }
                                    }
                                }
                            } else {
                                let _ = tx.send(Tool::AddValues { object_id, values: vals }).await;
                            }
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
