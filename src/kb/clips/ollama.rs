use crate::{
    CoCo, CoCoModule,
    db::Database,
    kb::{KnowledgeBase, clips::CLIPSKnowledgeBase},
    model::{CoCoError, CoCoProperty, CoCoValue, value_from_json},
};
use async_trait::async_trait;
use chrono::Utc;
use clips::{ClipsValue, Type, UDFContext};
use futures_util::StreamExt;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, trace, warn};

enum OllamaMessage {
    AddValues { object_id: String, values: HashMap<String, CoCoValue> },
    GetTools { tools: HashMap<String, HashSet<String>>, resp_tx: oneshot::Sender<Result<(HashMap<String, HashMap<String, CoCoProperty>>, Vec<Value>), CoCoError>> },
}

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
        let model = self.model.clone();
        let client = self.client.clone();
        let url = self.url.clone();
        let (values_tx, mut values_rx) = mpsc::unbounded_channel::<OllamaMessage>();
        let values_kb = kb.clone();

        tokio::spawn(async move {
            while let Some(update) = values_rx.recv().await {
                match update {
                    OllamaMessage::AddValues { object_id, values } => {
                        trace!("Received AddValues for object_id {}: values: {}", object_id, values.iter().map(|(k, v)| format!("{}={:?}", k, v)).collect::<Vec<_>>().join(", "));
                        if let Err(e) = values_kb.add_values(object_id.clone(), values, Utc::now()).await {
                            error!("Failed to add values to object {}: {}", object_id, e);
                        }
                    }
                    OllamaMessage::GetTools { tools, resp_tx } => {
                        trace!("Received GetTools request with tools: {:?}", tools);
                        match values_kb.get_dynamic_properties(tools.keys().cloned().collect()).await {
                            Ok(mut props) => {
                                props.retain(|class, class_props| {
                                    if let Some(requested_tools) = tools.get(class) {
                                        class_props.retain(|prop_name, _| requested_tools.contains(prop_name));
                                        !class_props.is_empty() // remove class if no properties left after filtering
                                    } else {
                                        false // remove class if not requested
                                    }
                                });
                                let mut tools = Vec::new();
                                for (class, class_props) in &props {
                                    if let Ok(class) = values_kb.get_class(class).await {
                                        let mut tool = json!({
                                            "name": class.name,
                                        });
                                        if let Some(desc) = &class.description {
                                            tool["description"] = Value::String(desc.clone());
                                        }
                                        let mut params = Map::new();
                                        for (prop_name, prop) in class_props {
                                            params.insert(
                                                prop_name.clone(),
                                                match prop {
                                                    CoCoProperty::Bool { description, .. } => {
                                                        let mut param = json!({
                                                            "type": "boolean",
                                                        });
                                                        if let Some(desc) = description {
                                                            param["description"] = Value::String(desc.clone());
                                                        }
                                                        param
                                                    }
                                                    CoCoProperty::Int { description, .. } => {
                                                        let mut param = json!({
                                                            "type": "integer",
                                                        });
                                                        if let Some(desc) = description {
                                                            param["description"] = Value::String(desc.clone());
                                                        }
                                                        param
                                                    }
                                                    CoCoProperty::Float { description, .. } => {
                                                        let mut param = json!({
                                                            "type": "number",
                                                        });
                                                        if let Some(desc) = description {
                                                            param["description"] = Value::String(desc.clone());
                                                        }
                                                        param
                                                    }
                                                    CoCoProperty::String { description, .. } => {
                                                        let mut param = json!({
                                                            "type": "string",
                                                        });
                                                        if let Some(desc) = description {
                                                            param["description"] = Value::String(desc.clone());
                                                        }
                                                        param
                                                    }
                                                    CoCoProperty::Symbol { description, allowed_values, .. } => {
                                                        let mut param = json!({
                                                            "type": "string",
                                                        });
                                                        if let Some(desc) = description {
                                                            param["description"] = Value::String(desc.clone());
                                                        }
                                                        if let Some(allowed) = allowed_values {
                                                            param["enum"] = Value::Array(allowed.iter().map(|v| Value::String(v.clone())).collect());
                                                        }
                                                        param
                                                    }
                                                    CoCoProperty::Object { classes, description, .. } => {
                                                        let mut param = json!({
                                                            "type": "string",
                                                        });
                                                        if let Some(desc) = description {
                                                            param["description"] = Value::String(desc.clone());
                                                        }
                                                        let mut instances = HashSet::new();
                                                        for class in classes {
                                                            match values_kb.get_class_instances(class).await {
                                                                Ok(class_instances) => {
                                                                    instances.extend(class_instances);
                                                                }
                                                                Err(e) => {
                                                                    error!("Failed to get instances for class {}: {}", class, e);
                                                                }
                                                            }
                                                        }
                                                        if !instances.is_empty() {
                                                            param["enum"] = Value::Array(instances.into_iter().map(|v| Value::String(v)).collect());
                                                        }
                                                        param
                                                    }
                                                    CoCoProperty::BoolArray { description, .. } => {
                                                        let mut param = json!({
                                                            "type": "array",
                                                            "items": { "type": "boolean" },
                                                        });
                                                        if let Some(desc) = description {
                                                            param["description"] = Value::String(desc.clone());
                                                        }
                                                        param
                                                    }
                                                    CoCoProperty::IntArray { description, .. } => {
                                                        let mut param = json!({
                                                            "type": "array",
                                                            "items": { "type": "integer" },
                                                        });
                                                        if let Some(desc) = description {
                                                            param["description"] = Value::String(desc.clone());
                                                        }
                                                        param
                                                    }
                                                    CoCoProperty::FloatArray { description, .. } => {
                                                        let mut param = json!({
                                                            "type": "array",
                                                            "items": { "type": "number" },
                                                        });
                                                        if let Some(desc) = description {
                                                            param["description"] = Value::String(desc.clone());
                                                        }
                                                        param
                                                    }
                                                    CoCoProperty::StringArray { description, .. } => {
                                                        let mut param = json!({
                                                            "type": "array",
                                                            "items": { "type": "string" },
                                                        });
                                                        if let Some(desc) = description {
                                                            param["description"] = Value::String(desc.clone());
                                                        }
                                                        param
                                                    }
                                                    CoCoProperty::SymbolArray { description, allowed_values, .. } => {
                                                        let mut param = json!({
                                                            "type": "array",
                                                            "items": { "type": "string" },
                                                        });
                                                        if let Some(desc) = description {
                                                            param["description"] = Value::String(desc.clone());
                                                        }
                                                        if let Some(allowed) = allowed_values {
                                                            param["enum"] = Value::Array(allowed.iter().map(|v| Value::String(v.clone())).collect());
                                                        }
                                                        param
                                                    }
                                                    CoCoProperty::ObjectArray { classes, description, .. } => {
                                                        let mut param = json!({
                                                            "type": "array",
                                                            "items": { "type": "string" },
                                                        });
                                                        if let Some(desc) = description {
                                                            param["description"] = Value::String(desc.clone());
                                                        }
                                                        let mut instances = HashSet::new();
                                                        for class in classes {
                                                            match values_kb.get_class_instances(class).await {
                                                                Ok(class_instances) => {
                                                                    instances.extend(class_instances);
                                                                }
                                                                Err(e) => {
                                                                    error!("Failed to get instances for class {}: {}", class, e);
                                                                }
                                                            }
                                                        }
                                                        if !instances.is_empty() {
                                                            param["enum"] = Value::Array(instances.into_iter().map(|v| Value::String(v)).collect());
                                                        }
                                                        param
                                                    }
                                                },
                                            );
                                        }
                                        tool["parameters"] = json!({
                                            "type": "object",
                                            "properties": params,
                                        });
                                        tools.push(tool);
                                    }
                                }
                                let _ = resp_tx.send(Ok((props, tools)));
                            }
                            Err(e) => {
                                error!("Failed to get dynamic properties for tools {:?}: {}", tools, e);
                                let _ = resp_tx.send(Err(CoCoError::KnowledgeBaseError(format!("Failed to get dynamic properties: {}", e))));
                            }
                        }
                    }
                }
            }
        });

        kb.add_udf(
            "prompt",
            vec![Type(Type::VOID)],
            2,
            2,
            vec![vec![Type(Type::STRING)], vec![Type(Type::STRING)]],
            Box::new(move |_env, ctx: &mut UDFContext| {
                let prompt = match ctx.get_next_argument(Type(Type::STRING)) {
                    Some(ClipsValue::String(s)) => s,
                    _ => {
                        error!("Expected string for prompt argument in prompt UDF");
                        return ClipsValue::Void();
                    }
                };
                let tools = match ctx.get_next_argument(Type(Type::STRING)) {
                    Some(ClipsValue::String(s)) => s,
                    _ => {
                        error!("Expected string for tools argument in prompt UDF");
                        return ClipsValue::Void();
                    }
                };
                let tools: HashMap<String, HashSet<String>> = match serde_json::from_str(&tools) {
                    Ok(t) => t,
                    Err(e) => {
                        error!("Failed to parse tools JSON in prompt UDF: {}", e);
                        return ClipsValue::Void();
                    }
                };

                let client = client.clone();
                let url = url.clone();
                let model = model.clone();
                let values_tx = values_tx.clone();
                tokio::spawn(async move {
                    let (resp_tx, resp_rx) = oneshot::channel();
                    let _ = values_tx.send(OllamaMessage::GetTools { tools: tools.clone(), resp_tx });
                    let (props, tools) = match resp_rx.await {
                        Ok(Ok(props)) => props,
                        Ok(Err(e)) => {
                            error!("Failed to get prompt context: {}", e);
                            return;
                        }
                        Err(e) => {
                            error!("Failed to receive prompt context response: {}", e);
                            return;
                        }
                    };
                    let body = json!({
                        "model": model,
                        "stream": true,
                        "messages": [
                            {
                                "role": "user",
                                "content": prompt,
                            }
                        ],
                        "tools": tools,
                    });
                    match client.post(&url).json(&body).send().await {
                        Ok(resp) => {
                            let mut byte_stream = resp.bytes_stream();
                            let mut line_buf: Vec<u8> = Vec::new();
                            while let Some(chunk) = byte_stream.next().await {
                                match chunk {
                                    Ok(bytes) => {
                                        line_buf.extend_from_slice(&bytes);
                                        while let Some(pos) = line_buf.iter().position(|&b| b == b'\n') {
                                            let line: Vec<u8> = line_buf.drain(..=pos).collect();
                                            let line = &line[..line.len().saturating_sub(1)];
                                            trace!("Received line from Ollama API: {:?}", String::from_utf8_lossy(line));

                                            if line.is_empty() {
                                                continue;
                                            }

                                            let mut parsed: ChatChunk = match serde_json::from_slice(line) {
                                                Ok(p) => p,
                                                Err(e) => {
                                                    error!("Failed to parse line from Ollama API: {}. Line: {:?}", e, String::from_utf8_lossy(line));
                                                    continue;
                                                }
                                            };

                                            for call in &mut parsed.message.tool_calls {
                                                let object_id = call.function.arguments.as_object_mut().and_then(|map| map.remove("object_id"));
                                                let object_id = match object_id {
                                                    Some(Value::String(s)) => s,
                                                    _ => {
                                                        warn!("Tool call missing or invalid object_id: {:?}", call.function.arguments);
                                                        continue;
                                                    }
                                                };
                                                let mut values = HashMap::new();
                                                for (key, value) in call.function.arguments.as_object().unwrap_or(&Map::new()) {
                                                    let prop = props.get(&call.function.name).and_then(|class_props| class_props.get(key));
                                                    if let Some(prop) = prop {
                                                        if let Ok(coco_value) = value_from_json(prop, value) {
                                                            values.insert(key.clone(), coco_value);
                                                        } else {
                                                            warn!("Failed to convert value for property {} in tool call {}: {:?}", key, call.function.name, value);
                                                        }
                                                    } else {
                                                        warn!("Property {} in tool call {} not found in knowledge base, skipping", key, call.function.name);
                                                        continue;
                                                    }
                                                }
                                                trace!("Received tool call for object_id {}: function: {}, arguments: {:?}", object_id, call.function.name, call.function.arguments);
                                                let _ = values_tx.send(OllamaMessage::AddValues { object_id, values });
                                            }

                                            if parsed.message.tool_calls.is_empty() && !parsed.message.content.is_empty() {
                                                warn!("Ollama API response contains content but no tool calls: {}", parsed.message.content);
                                            }

                                            if parsed.done {
                                                trace!("Ollama API response done: {:?}", parsed.done_reason);
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!("Error while reading response stream from Ollama API: {}", e);
                                        break;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to send request to Ollama API: {}", e);
                        }
                    }
                });
                ClipsValue::Void()
            }),
        )
        .await
        .map_err(|e| CoCoError::KnowledgeBaseError(format!("Failed to add prompt UDF: {}", e)))?;

        info!("OllamaModule initialized with model '{}'", self.model);
        Ok(())
    }
}

#[derive(Deserialize)]
struct ChatChunk {
    message: Message,
    done: bool,
    #[serde(default)]
    done_reason: Option<String>,
}

#[derive(Deserialize)]
struct Message {
    #[serde(default)]
    content: String,
    #[serde(default)]
    tool_calls: Vec<ToolCall>,
}

#[derive(Deserialize)]
struct ToolCall {
    function: FunctionCall,
}

#[derive(Deserialize)]
struct FunctionCall {
    name: String,
    arguments: Value,
}
