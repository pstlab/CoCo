use crate::{
    kb::{CLIPSKnowledgeBase, KnowledgeBase, KnowledgeBaseError},
    model::{Property, Value, value_from_json},
};
use chrono::Utc;
use clips::{ClipsValue, Type};
use reqwest::Client;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;
use tracing::{error, info, trace};

enum Tool {
    SetProperties { object_id: String, properties: HashMap<String, Value> },
    AddValues { object_id: String, values: HashMap<String, Value> },
}

fn props_to_params(root_name: &str, props: &HashMap<String, HashMap<String, Property>>) -> serde_json::Value {
    let mut class_entries = serde_json::Map::new();

    for (class_name, class_props) in props {
        let mut prop_entries = serde_json::Map::new();
        for (prop_name, _property) in class_props {
            prop_entries.insert(
                prop_name.clone(),
                serde_json::json!({
                    "anyOf": [
                        { "type": "string" },
                        { "type": "integer" },
                        { "type": "number" },
                        { "type": "boolean" },
                        { "type": "array" },
                        { "type": "null" }
                    ]
                }),
            );
        }

        class_entries.insert(
            class_name.clone(),
            serde_json::json!({
                "type": "object",
                "properties": prop_entries,
                "additionalProperties": false
            }),
        );
    }

    let mut root_properties = serde_json::Map::new();
    root_properties.insert(
        root_name.to_string(),
        serde_json::json!({
            "type": "object",
            "description": "Mappa classe -> mappa nome_proprieta -> valore",
            "properties": class_entries,
            "additionalProperties": false
        }),
    );

    serde_json::json!({
        "type": "object",
        "properties": root_properties,
        "required": [root_name],
        "additionalProperties": false
    })
}

fn collect_typed_values(schema: &HashMap<String, HashMap<String, Property>>, arguments: &serde_json::Value) -> HashMap<String, Value> {
    let mut collected = HashMap::new();

    let Some(class_entries) = arguments.as_object() else {
        return collected;
    };

    for (class_name, raw_props) in class_entries {
        let Some(class_schema) = schema.get(class_name) else {
            error!("Unknown class '{}' in tool arguments", class_name);
            continue;
        };

        let Some(raw_props) = raw_props.as_object() else {
            error!("Expected object for class '{}', got {}", class_name, raw_props);
            continue;
        };

        for (prop_name, raw_value) in raw_props {
            let Some(property) = class_schema.get(prop_name) else {
                error!("Unknown property '{}.{}' in tool arguments", class_name, prop_name);
                continue;
            };

            match value_from_json(property, raw_value) {
                Ok(value) => {
                    collected.insert(prop_name.clone(), value);
                }
                Err(e) => {
                    error!("Invalid value for '{}.{}': {} -- raw: {}", class_name, prop_name, e, raw_value);
                }
            }
        }
    }

    collected
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

    let udf_kb_clone = kb.clone();
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

            let async_kb_clone = udf_kb_clone.clone();
            tokio::spawn(async move {
                let classes = async_kb_clone.get_object_classes(object_id.clone()).await.unwrap_or_else(|e| {
                    error!("Failed to get classes for object_id {}: {}", object_id, e);
                    HashSet::new()
                });
                let static_props = async_kb_clone.get_static_properties(classes.clone()).await.unwrap_or_else(|e| {
                    error!("Failed to get static properties for object_id {}: {}", object_id, e);
                    HashMap::new()
                });
                let dynamic_props = async_kb_clone.get_dynamic_properties(classes.clone()).await.unwrap_or_else(|e| {
                    error!("Failed to get dynamic properties for object_id {}: {}", object_id, e);
                    HashMap::new()
                });

                trace!("Sending prompt to Ollama for object_id {}: {}", object_id, prompt);
                let body = serde_json::json!({
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "stream": false,
                    "tools": [
                        {
                            "type": "function",
                            "function": {
                                "name": "set_properties",
                                "description": "Set properties on an object in the knowledge base. The properties to set are provided in the 'properties' argument as a dictionary of property names to values.",
                                "parameters": props_to_params("properties", &static_props)
                            }
                        },
                        {
                            "type": "function",
                            "function": {
                                "name": "add_values",
                                "description": "Add values to properties on an object in the knowledge base. The values to add are provided in the 'values' argument as a dictionary of property names to values.",
                                "parameters": props_to_params("values", &dynamic_props)
                            }
                        }
                    ]
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
                            if let Some(tools) = json["tool_calls"].as_array() {
                                for tool in tools {
                                    let tool_name = tool["function"]["name"].as_str().unwrap_or_else(|| {
                                        error!("Tool call without function name in Ollama response for object_id {}: {}", object_id, json);
                                        "unknown_tool"
                                    });

                                    let arguments = if let Some(args) = tool["function"]["arguments"].as_object() {
                                        serde_json::Value::Object(args.clone())
                                    } else {
                                        error!("Tool call without arguments object in Ollama response for object_id {}: {}", object_id, json);
                                        serde_json::Value::Null
                                    };

                                    match tool_name {
                                        "set_properties" => {
                                            let nested = arguments.get("properties").unwrap_or(&arguments);
                                            let props = collect_typed_values(&static_props, nested);
                                            if !props.is_empty() {
                                                let _ = tx.send(Tool::SetProperties { object_id: object_id.clone(), properties: props }).await;
                                            }
                                        }
                                        "add_values" => {
                                            let nested = arguments.get("values").unwrap_or(&arguments);
                                            let vals = collect_typed_values(&dynamic_props, nested);
                                            if !vals.is_empty() {
                                                let _ = tx.send(Tool::AddValues { object_id: object_id.clone(), values: vals }).await;
                                            }
                                        }
                                        _ => {
                                            error!("Unknown tool called by Ollama for object_id {}: {}", object_id, tool_name);
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
