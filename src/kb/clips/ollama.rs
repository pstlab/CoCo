use crate::{
    CoCo, CoCoModule,
    db::Database,
    kb::{KnowledgeBase, clips::CLIPSKnowledgeBase},
    model::{CoCoError, CoCoProperty, CoCoValue},
};
use async_trait::async_trait;
use chrono::Utc;
use clips::{ClipsValue, Type, UDFContext};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, trace};

enum OllamaMessage {
    AddValues { object_id: String, text: String, values: HashMap<String, CoCoValue> },
    GetPromptContext { tools: HashMap<String, HashSet<String>>, resp_tx: oneshot::Sender<Result<HashMap<String, HashMap<String, CoCoProperty>>, CoCoError>> },
}

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
        let (values_tx, mut values_rx) = mpsc::unbounded_channel::<OllamaMessage>();
        let values_kb = kb.clone();

        tokio::spawn(async move {
            while let Some(update) = values_rx.recv().await {
                match update {
                    OllamaMessage::AddValues { object_id, text, values } => {
                        trace!("Received AddValues for object_id {}: text: {}, values: {}", object_id, text, values.iter().map(|(k, v)| format!("{}={:?}", k, v)).collect::<Vec<_>>().join(", "));
                        let timestamp = Utc::now();
                        if let Err(e) = values_kb.add_values(object_id.clone(), values, timestamp).await {
                            error!("Failed to add values to object {}: {}", object_id, e);
                        }
                    }
                    OllamaMessage::GetPromptContext { tools, resp_tx } => {
                        trace!("Received GetPromptContext request with tools: {:?}", tools);
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
                                let _ = resp_tx.send(Ok(props));
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
            None,
            2,
            2,
            vec![Type(Type::STRING), Type(Type::STRING)],
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
                    let _ = values_tx.send(OllamaMessage::GetPromptContext { tools: tools.clone(), resp_tx });
                    let prompt_context = match resp_rx.await {
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
    arguments: serde_json::Value,
}

#[derive(Serialize)]
struct ChatRequest<'a> {
    model: &'a str,
    stream: bool,
    messages: Vec<serde_json::Value>,
    tools: Vec<serde_json::Value>,
    tool_choice: &'a str,
}
