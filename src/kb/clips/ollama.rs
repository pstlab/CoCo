use crate::{
    CoCo, CoCoModule,
    db::Database,
    kb::{KnowledgeBase, clips::CLIPSKnowledgeBase},
    model::{CoCoError, Property, Value},
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clips::{ClipsValue, Type, UDFContext};
use futures_util::StreamExt;
use reqwest::{Client, Response};
use serde::Deserialize;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, trace};

pub struct OllamaModule {
    model: String,
    url: String,
    client: Client,
}

enum OllamaMessage {
    AddValues { object_id: String, values: HashMap<String, Value>, timestamp: DateTime<Utc> },
    GetPromptContext { object_id: String, resp_tx: oneshot::Sender<Result<HashMap<String, Property>, CoCoError>> },
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
                    OllamaMessage::AddValues { object_id, values, timestamp } => {
                        trace!("Received AddValues for object_id {}: {:?}", object_id, values);
                        if let Err(e) = values_kb.add_values(object_id.clone(), values, timestamp).await {
                            error!("Failed to add values to object {}: {}", object_id, e);
                        }
                    }
                    OllamaMessage::GetPromptContext { object_id, resp_tx } => {
                        trace!("Received GetPromptContext for object_id {}", object_id);
                        match values_kb.get_object(object_id.clone()).await {
                            Ok(Some(object)) => match values_kb.get_dynamic_properties(object.classes).await {
                                Ok(props) => {
                                    let _ = resp_tx.send(Ok(props.into_values().flat_map(|m| m).collect()));
                                }
                                Err(e) => {
                                    let _ = resp_tx.send(Err(CoCoError::KnowledgeBaseError(format!("Failed to get prompt context for object {}: {}", object_id, e))));
                                }
                            },
                            Ok(None) => {
                                let _ = resp_tx.send(Err(CoCoError::KnowledgeBaseError(format!("Object {} not found", object_id))));
                                continue;
                            }
                            Err(e) => {
                                let _ = resp_tx.send(Err(CoCoError::KnowledgeBaseError(format!("Failed to get object {}: {}", object_id, e))));
                                continue;
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
            vec![Type(Type::SYMBOL), Type(Type::STRING)],
            Box::new(move |_env, ctx: &mut UDFContext| {
                let object_id = match ctx.get_next_argument(Type(Type::SYMBOL)) {
                    Some(ClipsValue::Symbol(s)) => s,
                    _ => {
                        error!("Expected symbol for object ID argument in prompt UDF");
                        return ClipsValue::Void();
                    }
                };

                let prompt = match ctx.get_next_argument(Type(Type::STRING)) {
                    Some(ClipsValue::String(s)) => s,
                    _ => {
                        error!("Expected string for prompt argument in prompt UDF");
                        return ClipsValue::Void();
                    }
                };

                let client = client.clone();
                let url = url.clone();
                let model = model.clone();
                let values_tx = values_tx.clone();
                tokio::spawn(async move {
                    let (resp_tx, resp_rx) = oneshot::channel();
                    let _ = values_tx.send(OllamaMessage::GetPromptContext { object_id: object_id.clone(), resp_tx });
                    let prompt_context = match resp_rx.await {
                        Ok(Ok(props)) => props,
                        Ok(Err(e)) => {
                            error!("Failed to get prompt context for object {}: {}", object_id, e);
                            return;
                        }
                        Err(e) => {
                            error!("Failed to receive prompt context for object {}: {}", object_id, e);
                            return;
                        }
                    };

                    let final_prompt = build_tagged_prompt(&prompt, &prompt_context);

                    trace!("Sending prompt to Ollama for object_id {}: {}", object_id, final_prompt);
                    let body = serde_json::json!({
                        "model": model,
                        "prompt": final_prompt
                    });

                    match client.post(&url).json(&body).send().await {
                        Ok(response) => {
                            parse_response(object_id, response, values_tx).await;
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

async fn parse_response(object_id: String, response: Response, values_tx: mpsc::UnboundedSender<OllamaMessage>) {
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
                Ok(line_str) => {
                    let done = parse_ollama_line(line_str, &mut full_text);
                    flush_values(&object_id, &mut full_text, done, &values_tx);
                }
                Err(e) => error!("Invalid UTF-8 sequence in line: {}", e),
            }
        }
    }

    if !pending.is_empty() {
        if let Ok(line_str) = std::str::from_utf8(&pending) {
            let trimmed = line_str.trim_end_matches(['\r', '\n']);
            if !trimmed.is_empty() {
                let done = parse_ollama_line(trimmed, &mut full_text);
                flush_values(&object_id, &mut full_text, done, &values_tx);
            }
        }
    }
}

fn parse_ollama_line(line: &str, full_text: &mut String) -> bool {
    match serde_json::from_str::<OllamaResponse>(line) {
        Ok(ollama_response) => {
            trace!("Parsed Ollama response: {:?}", ollama_response.response);
            full_text.push_str(&ollama_response.response);
            ollama_response.done
        }
        Err(e) => {
            error!("Error parsing Ollama response: {}", e);
            false
        }
    }
}

enum ParserState {
    Text,
    Command,
}

fn flush_values(object_id: &str, full_text: &mut String, done: bool, values_tx: &mpsc::UnboundedSender<OllamaMessage>) {
    let mut state = ParserState::Text;
    let mut buffer = String::new();
    let mut safe_cut_index = 0;
    let mut values = HashMap::new();
    for (idx, c) in full_text.char_indices() {
        match state {
            ParserState::Text => {
                if c == '<' {
                    let text = buffer.trim();
                    if !text.is_empty() {
                        values.insert("text".to_string(), Value::String(text.to_string()));
                        trace!("Sending values to OllamaUpdate: {:?}", values);
                        let _ = values_tx.send(OllamaMessage::AddValues { object_id: object_id.to_string(), values: values.clone(), timestamp: Utc::now() });
                        values.remove("text");
                    }

                    // Everything before '<' is finalized text, so we can safely drain it
                    // even if the tag itself is incomplete in this chunk.
                    safe_cut_index = safe_cut_index.max(idx);

                    buffer.clear();
                    state = ParserState::Command;
                } else {
                    buffer.push(c);
                    if c == '.' || c == '?' || c == '!' {
                        let text = buffer.trim();
                        if !text.is_empty() {
                            values.insert("text".to_string(), Value::String(text.to_string()));
                            trace!("Sending values to OllamaUpdate: {:?}", values);
                            let _ = values_tx.send(OllamaMessage::AddValues { object_id: object_id.to_string(), values: values.clone(), timestamp: Utc::now() });
                            values.remove("text");
                        }
                        buffer.clear();
                        safe_cut_index = idx + c.len_utf8();
                    }
                }
            }
            ParserState::Command => {
                if c == '>' {
                    for part in buffer.split(|ch| [';', ','].contains(&ch)) {
                        let pair = part.trim();
                        if pair.is_empty() {
                            continue;
                        }
                        let mut key_val = pair.splitn(2, '=');
                        let key = key_val.next().unwrap_or("").trim();
                        let val = key_val.next().unwrap_or("").trim_matches(|ch| ch == '"' || ch == '\'').trim();
                        if !key.is_empty() && !val.is_empty() {
                            values.insert(key.to_string(), Value::String(val.to_string()));
                        }
                    }

                    buffer.clear();
                    state = ParserState::Text;
                } else {
                    buffer.push(c);
                }
            }
        }
    }

    if done {
        if let ParserState::Text = state {
            let text = buffer.trim();
            if !text.is_empty() {
                values.insert("text".to_string(), Value::String(text.to_string()));
                trace!("Sending values to OllamaUpdate: {:?}", values);
                let _ = values_tx.send(OllamaMessage::AddValues { object_id: object_id.to_string(), values: values.clone(), timestamp: Utc::now() });
                values.remove("text");
            }
            safe_cut_index = full_text.len();
        }
    }

    full_text.drain(..safe_cut_index);
}

fn property_description(property: &Property) -> String {
    match property {
        Property::Bool { description: Some(description), .. } => format!("Bool property, {}", description),
        Property::String { description: Some(description), .. } => format!("String property, {}", description),
        Property::Symbol { allowed_values: Some(allowed_values), description: Some(description), .. } => format!("Symbol property (allowed values: {}), {}", allowed_values.iter().cloned().collect::<Vec<_>>().join(", "), description),
        Property::Int { min, max, description: Some(description), .. } => format!("Int property (min: {}, max: {}), {}", min.map_or("-∞".to_string(), |v| v.to_string()), max.map_or("∞".to_string(), |v| v.to_string()), description),
        Property::Float { min, max, description: Some(description), .. } => format!("Float property (min: {}, max: {}), {}", min.map_or("-∞".to_string(), |v| v.to_string()), max.map_or("∞".to_string(), |v| v.to_string()), description),
        _ => "No description".to_string(),
    }
}

fn build_tagged_prompt(user_prompt: &str, properties: &HashMap<String, Property>) -> String {
    let allowed_keys = properties.keys().cloned().collect::<Vec<_>>().join(", ");

    let property_details = properties.iter().map(|(name, property)| format!("- {} => {}", name, property_description(property))).collect::<Vec<_>>().join("\n");

    let tag_requirement = if properties.is_empty() { "- If no allowed keys are available, return plain text without tags." } else { "- Include at least one tag when the sentence has any expressive or stylistic cue." };

    format!(
        "You are generating expressive utterances.\n\
TASK\n\
- Produce one response where text can include inline tags before the text they modify.\n\
\n\
TAG FORMAT\n\
- Use tags like <key=value> or <key=value;arg2=value2>.\n\
- Keep tags compact with no spaces inside tags.\n\
- A tag modifies the text that follows it.\n\
\n\
ALLOWED KEYS\n\
{}\n\
\n\
PROPERTY DETAILS\n\
{}\n\
\n\
OUTPUT RULES\n\
- Return only tagged text, no explanations.\n\
- Use only allowed keys.\n\
- Prefer sentence-level tags and only when meaningful.\n\
{}\n\
\n\
Now answer this user message:\n\
{}",
        allowed_keys, property_details, tag_requirement, user_prompt
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        kb::KnowledgeBase,
        model::{Class, Object, Property},
    };
    use std::collections::{HashMap, HashSet};
    use tokio::sync::mpsc;
    use tracing::{Level, subscriber};

    #[tokio::test]
    async fn test_ollama_connection() {
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
                assert!(response.status().is_success(), "Ollama API request failed with status: {}", response.status());
            }
            Err(e) => {
                panic!("Failed to send request to Ollama: {}", e);
            }
        };
    }

    #[tokio::test]
    async fn test_flush_values() {
        let (values_tx, mut values_rx) = mpsc::unbounded_channel();
        let object_id = "test_object".to_string();
        let mut full_text = String::from("Hello world. <facial=happy;arms=opened> How are you? <facial=sad, arms=crossed> Goodbye!");

        flush_values(&object_id, &mut full_text, true, &values_tx);

        let mut received_values = Vec::new();
        while let Ok(update) = values_rx.try_recv() {
            if let OllamaMessage::AddValues { object_id: _, values, timestamp: _ } = update {
                received_values.push(values);
            }
        }

        assert_eq!(received_values.len(), 3);
        assert_eq!(received_values[0].get("text").unwrap(), &Value::String("Hello world.".to_string()));
        assert_eq!(received_values[1].get("facial").unwrap(), &Value::String("happy".to_string()));
        assert_eq!(received_values[1].get("arms").unwrap(), &Value::String("opened".to_string()));
        assert_eq!(received_values[1].get("text").unwrap(), &Value::String("How are you?".to_string()));
        assert_eq!(received_values[2].get("facial").unwrap(), &Value::String("sad".to_string()));
        assert_eq!(received_values[2].get("arms").unwrap(), &Value::String("crossed".to_string()));
        assert_eq!(received_values[2].get("text").unwrap(), &Value::String("Goodbye!".to_string()));
    }

    #[tokio::test]
    async fn test_flush_values_only_text_on_done() {
        let (values_tx, mut values_rx) = mpsc::unbounded_channel();
        let object_id = "test_object".to_string();
        let mut full_text = String::from("Solo testo senza punteggiatura finale");

        flush_values(&object_id, &mut full_text, true, &values_tx);

        let mut received_values = Vec::new();
        while let Ok(update) = values_rx.try_recv() {
            if let OllamaMessage::AddValues { object_id: _, values, timestamp: _ } = update {
                received_values.push(values);
            }
        }

        assert_eq!(received_values.len(), 1);
        assert_eq!(received_values[0].get("text").unwrap(), &Value::String("Solo testo senza punteggiatura finale".to_string()));
        assert!(full_text.is_empty());
    }

    #[tokio::test]
    async fn test_flush_values_no_duplicate_when_tag_is_streamed() {
        let (values_tx, mut values_rx) = mpsc::unbounded_channel();
        let object_id = "test_object".to_string();
        let mut full_text = String::new();

        full_text.push_str("Hello <fac");
        flush_values(&object_id, &mut full_text, false, &values_tx);

        full_text.push_str("ial=happy>");
        flush_values(&object_id, &mut full_text, false, &values_tx);

        full_text.push_str(" world!");
        flush_values(&object_id, &mut full_text, true, &values_tx);

        let mut received_values = Vec::new();
        while let Ok(update) = values_rx.try_recv() {
            if let OllamaMessage::AddValues { object_id: _, values, timestamp: _ } = update {
                received_values.push(values);
            }
        }

        assert_eq!(received_values.len(), 2);
        assert_eq!(received_values[0].get("text").unwrap(), &Value::String("Hello".to_string()));
        assert_eq!(received_values[1].get("facial").unwrap(), &Value::String("happy".to_string()));
        assert_eq!(received_values[1].get("text").unwrap(), &Value::String("world!".to_string()));
    }

    #[tokio::test]
    async fn test_parse_response() {
        let subscriber = tracing_subscriber::fmt().with_max_level(Level::TRACE).finish();
        subscriber::set_global_default(subscriber).expect("Failed to set global default subscriber");

        let (kb, _) = CLIPSKnowledgeBase::new();
        kb.create_class(Class {
            name: "TestClass".to_string(),
            static_properties: None,
            dynamic_properties: Some(HashMap::from([
                ("text".to_string(), Property::String { default: None, description: None }),
                (
                    "facial".to_string(),
                    Property::Symbol {
                        default: Some("neutral".to_string()),
                        allowed_values: Some(HashSet::from(["neutral".to_string(), "happy".to_string(), "sad".to_string()])),
                        description: Some("Facial expression of the object".to_string()),
                    },
                ),
            ])),
            parents: None,
        })
        .await
        .unwrap();
        kb.create_object(Object {
            id: Some("test_object".to_string()),
            classes: HashSet::from(["TestClass".to_string()]),
            properties: None,
            values: None,
        })
        .await
        .unwrap();

        let props = kb.get_dynamic_properties(HashSet::from(["TestClass".to_string()])).await.expect("Failed to get dynamic properties").into_values().flat_map(|m| m).collect::<HashMap<String, Property>>();

        let host = std::env::var("OLLAMA_HOST").unwrap_or_else(|_| "localhost".to_string());
        let port = std::env::var("OLLAMA_PORT").unwrap_or_else(|_| "11434".to_string()).parse::<u16>().unwrap_or(11434);
        let model = std::env::var("OLLAMA_MODEL").unwrap_or_else(|_| "llama3".to_string());
        let url = format!("http://{}:{}/api/generate", host, port);
        let client = Client::new();
        let body = serde_json::json!({
            "model": model,
            "prompt": build_tagged_prompt("Test prompt for Ollama response parsing.", &props)
        });

        match client.post(&url).json(&body).send().await {
            Ok(response) => {
                let (values_tx, _values_rx) = mpsc::unbounded_channel();
                parse_response("test_object".to_string(), response, values_tx).await;
            }
            Err(_) => {
                error!("Failed to send request to Ollama for test: {}", url);
            }
        };
    }
}
