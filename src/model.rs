use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{
    collections::{HashMap, HashSet},
    fmt,
};
use tracing::error;
#[cfg(feature = "server")]
use utoipa::ToSchema;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[serde(tag = "type")]
pub enum Property {
    #[serde(rename = "bool")]
    Bool {
        #[serde(skip_serializing_if = "Option::is_none")]
        default: Option<bool>,
    },
    #[serde(rename = "int")]
    Int {
        #[serde(skip_serializing_if = "Option::is_none")]
        default: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        min: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max: Option<i64>,
    },
    #[serde(rename = "float")]
    Float {
        #[serde(skip_serializing_if = "Option::is_none")]
        default: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        min: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max: Option<f64>,
    },
    #[serde(rename = "string")]
    String {
        #[serde(skip_serializing_if = "Option::is_none")]
        default: Option<String>,
    },
    #[serde(rename = "symbol")]
    Symbol {
        #[serde(skip_serializing_if = "Option::is_none")]
        default: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        allowed_values: Option<HashSet<String>>,
    },
    #[serde(rename = "object")]
    Object {
        #[serde(skip_serializing_if = "Option::is_none")]
        default: Option<String>,
        class: String,
    },
    #[serde(rename = "bool-array")]
    BoolArray {
        #[serde(skip_serializing_if = "Option::is_none")]
        default: Option<Vec<bool>>,
    },
    #[serde(rename = "int-array")]
    IntArray {
        #[serde(skip_serializing_if = "Option::is_none")]
        default: Option<Vec<i64>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        min: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max: Option<i64>,
    },
    #[serde(rename = "float-array")]
    FloatArray {
        #[serde(skip_serializing_if = "Option::is_none")]
        default: Option<Vec<f64>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        min: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max: Option<f64>,
    },
    #[serde(rename = "string-array")]
    StringArray {
        #[serde(skip_serializing_if = "Option::is_none")]
        default: Option<Vec<String>>,
    },
    #[serde(rename = "symbol-array")]
    SymbolArray {
        #[serde(skip_serializing_if = "Option::is_none")]
        default: Option<Vec<String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        allowed_values: Option<HashSet<String>>,
    },
    #[serde(rename = "object-array")]
    ObjectArray {
        #[serde(skip_serializing_if = "Option::is_none")]
        default: Option<Vec<String>>,
        class: String,
    },
}

pub fn from_json(property: &Property, raw: &JsonValue) -> Result<Value, CoCoError> {
    match property {
        Property::Bool { .. } => raw.as_bool().map(Value::Bool).ok_or_else(|| CoCoError::JsonParseError(format!("Expected bool, got: {}", raw))),
        Property::Int { .. } => raw.as_i64().map(Value::Int).ok_or_else(|| CoCoError::JsonParseError(format!("Expected int, got: {}", raw))),
        Property::Float { .. } => raw.as_f64().map(Value::Float).ok_or_else(|| CoCoError::JsonParseError(format!("Expected float, got: {}", raw))),
        Property::String { .. } => raw.as_str().map(|s| Value::String(s.to_string())).ok_or_else(|| CoCoError::JsonParseError(format!("Expected string, got: {}", raw))),
        Property::Symbol { .. } => raw.as_str().map(|s| Value::Symbol(s.to_string())).ok_or_else(|| CoCoError::JsonParseError(format!("Expected symbol, got: {}", raw))),
        Property::Object { .. } => raw.as_str().map(|s| Value::Object(s.to_string())).ok_or_else(|| CoCoError::JsonParseError(format!("Expected object reference, got: {}", raw))),
        Property::BoolArray { .. } => raw
            .as_array()
            .and_then(|arr| {
                if !arr.iter().all(serde_json::Value::is_boolean) {
                    error!("Expected bool array, but found non-boolean value in array: {}", raw);
                    return None;
                }
                Some(Value::BoolArray(arr.iter().filter_map(serde_json::Value::as_bool).collect()))
            })
            .ok_or_else(|| CoCoError::JsonParseError(format!("Expected bool array, got: {}", raw))),
        Property::IntArray { .. } => raw
            .as_array()
            .and_then(|arr| {
                if !arr.iter().all(serde_json::Value::is_i64) {
                    error!("Expected int array, but found non-integer value in array: {}", raw);
                    return None;
                }
                Some(Value::IntArray(arr.iter().filter_map(serde_json::Value::as_i64).collect()))
            })
            .ok_or_else(|| CoCoError::JsonParseError(format!("Expected int array, got: {}", raw))),
        Property::FloatArray { .. } => raw
            .as_array()
            .and_then(|arr| {
                if !arr.iter().all(serde_json::Value::is_f64) {
                    error!("Expected float array, but found non-float value in array: {}", raw);
                    return None;
                }
                Some(Value::FloatArray(arr.iter().filter_map(serde_json::Value::as_f64).collect()))
            })
            .ok_or_else(|| CoCoError::JsonParseError(format!("Expected float array, got: {}", raw))),
        Property::StringArray { .. } => raw
            .as_array()
            .and_then(|arr| {
                if !arr.iter().all(serde_json::Value::is_string) {
                    error!("Expected string array, but found non-string value in array: {}", raw);
                    return None;
                }
                Some(Value::StringArray(arr.iter().filter_map(serde_json::Value::as_str).map(ToOwned::to_owned).collect()))
            })
            .ok_or_else(|| CoCoError::JsonParseError(format!("Expected string array, got: {}", raw))),
        Property::SymbolArray { .. } => raw
            .as_array()
            .and_then(|arr| {
                if !arr.iter().all(serde_json::Value::is_string) {
                    error!("Expected symbol array, but found non-string value in array: {}", raw);
                    return None;
                }
                Some(Value::SymbolArray(arr.iter().filter_map(serde_json::Value::as_str).map(ToOwned::to_owned).collect()))
            })
            .ok_or_else(|| CoCoError::JsonParseError(format!("Expected symbol array, got: {}", raw))),
        Property::ObjectArray { .. } => raw
            .as_array()
            .and_then(|arr| {
                if !arr.iter().all(serde_json::Value::is_string) {
                    error!("Expected object array, but found non-string value in array: {}", raw);
                    return None;
                }
                Some(Value::ObjectArray(arr.iter().filter_map(serde_json::Value::as_str).map(ToOwned::to_owned).collect()))
            })
            .ok_or_else(|| CoCoError::JsonParseError(format!("Expected object array, got: {}", raw))),
    }
}

impl fmt::Display for Property {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Bool { default } => {
                write!(f, "bool(default: {:?})", default)
            }
            Property::Int { default, min, max } => {
                write!(f, "int(default: {:?}, min: {:?}, max: {:?})", default, min, max)
            }
            Property::Float { default, min, max } => {
                write!(f, "float(default: {:?}, min: {:?}, max: {:?})", default, min, max)
            }
            Property::String { default } => {
                write!(f, "string(default: {:?})", default)
            }
            Property::Symbol { default, allowed_values } => {
                write!(f, "symbol(default: {:?}, allowed_values: {:?})", default, allowed_values)
            }
            Property::Object { default, class } => {
                write!(f, "object(default: {:?}, class: {:?})", default, class)
            }
            Property::BoolArray { default } => {
                write!(f, "bool-array(default: {:?})", default)
            }
            Property::IntArray { default, min, max } => {
                write!(f, "int-array(default: {:?}, min: {:?}, max: {:?})", default, min, max)
            }
            Property::FloatArray { default, min, max } => {
                write!(f, "float-array(default: {:?}, min: {:?}, max: {:?})", default, min, max)
            }
            Property::StringArray { default } => {
                write!(f, "string-array(default: {:?})", default)
            }
            Property::SymbolArray { default, allowed_values } => {
                write!(f, "symbol-array(default: {:?}, allowed_values: {:?})", default, allowed_values)
            }
            Property::ObjectArray { default, class } => {
                write!(f, "object-array(default: {:?}, class: {:?})", default, class)
            }
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[serde(untagged)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Symbol(String),
    Object(String),
    BoolArray(Vec<bool>),
    IntArray(Vec<i64>),
    FloatArray(Vec<f64>),
    StringArray(Vec<String>),
    SymbolArray(Vec<String>),
    ObjectArray(Vec<String>),
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct TimedValue {
    pub value: Value,
    pub timestamp: DateTime<Utc>,
}

impl PartialEq<&str> for Value {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Value::Null => *other == "null",
            Value::Bool(b) => other == &b.to_string(),
            Value::Int(i) => other == &i.to_string(),
            Value::Float(f) => other == &f.to_string(),
            Value::String(s) => other == s,
            Value::Symbol(s) => other == s,
            Value::Object(o) => other == o,
            Value::BoolArray(arr) => other == &format!("{:?}", arr),
            Value::IntArray(arr) => other == &format!("{:?}", arr),
            Value::FloatArray(arr) => other == &format!("{:?}", arr),
            Value::StringArray(arr) => other == &format!("{:?}", arr),
            Value::SymbolArray(arr) => other == &format!("{:?}", arr),
            Value::ObjectArray(arr) => other == &format!("{:?}", arr),
        }
    }
}

impl PartialEq<String> for Value {
    fn eq(&self, other: &String) -> bool {
        match self {
            Value::Null => other == "null",
            Value::Bool(b) => other == &b.to_string(),
            Value::Int(i) => other == &i.to_string(),
            Value::Float(f) => other == &f.to_string(),
            Value::String(s) => other == s,
            Value::Symbol(s) => other == s,
            Value::Object(o) => other == o,
            Value::BoolArray(arr) => other == &format!("{:?}", arr),
            Value::IntArray(arr) => other == &format!("{:?}", arr),
            Value::FloatArray(arr) => other == &format!("{:?}", arr),
            Value::StringArray(arr) => other == &format!("{:?}", arr),
            Value::SymbolArray(arr) => other == &format!("{:?}", arr),
            Value::ObjectArray(arr) => other == &format!("{:?}", arr),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Int(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::String(s) => write!(f, "\"{}\"", s),
            Value::Symbol(s) => write!(f, "'{}'", s),
            Value::Object(o) => write!(f, "object_id: {}", o),
            Value::BoolArray(arr) => write!(f, "bool_array: {:?}", arr),
            Value::IntArray(arr) => write!(f, "int_array: {:?}", arr),
            Value::FloatArray(arr) => write!(f, "float_array: {:?}", arr),
            Value::StringArray(arr) => write!(f, "string_array: {:?}", arr),
            Value::SymbolArray(arr) => write!(f, "symbol_array: {:?}", arr),
            Value::ObjectArray(arr) => write!(f, "object_array: {:?}", arr),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct Class {
    pub name: String,
    pub parents: Option<HashSet<String>>,
    pub static_properties: Option<HashMap<String, Property>>,
    pub dynamic_properties: Option<HashMap<String, Property>>,
}

impl fmt::Display for Class {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "class {} parents: {:?} static_properties: {:?} dynamic_properties: {:?}", self.name, self.parents, self.static_properties, self.dynamic_properties)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct Object {
    pub id: Option<String>,
    pub classes: HashSet<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<HashMap<String, TimedValue>>,
}

impl fmt::Display for Object {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "object {} classes: {:?} properties: {:?} values: {:?}", self.id.as_deref().unwrap_or(""), self.classes, self.properties, self.values)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct Rule {
    pub name: String,
    pub content: String,
}

impl fmt::Display for Rule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "rule {} content: {}", self.name, self.content)
    }
}

#[derive(Clone, Debug, Serialize)]
pub enum CoCoEvent {
    ClassCreated(String),                                       // class_name
    ObjectCreated(String),                                      // object_id
    AddedClass(String, String),                                 // (object_id, class_name)
    UpdatedProperties(String, HashMap<String, Value>),          // (object_id, properties)
    AddedValues(String, HashMap<String, Value>, DateTime<Utc>), // (object_id, value, date_time)
    RuleCreated(String),                                        // rule_name
}

impl fmt::Display for CoCoEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CoCoEvent::ClassCreated(class) => write!(f, "ClassCreated: {}", class),
            CoCoEvent::ObjectCreated(object) => write!(f, "ObjectCreated: {}", object),
            CoCoEvent::AddedClass(object, class) => write!(f, "AddedClass: {} to {}", class, object),
            CoCoEvent::UpdatedProperties(object, properties) => write!(f, "UpdatedProperties for {}: {:?}", object, properties),
            CoCoEvent::AddedValues(object, values, date_time) => write!(f, "AddedValues to {}: {:?} at {}", object, values, date_time),
            CoCoEvent::RuleCreated(rule) => write!(f, "RuleCreated: {}", rule),
        }
    }
}

#[derive(Clone, Debug)]
pub enum CoCoError {
    ConfigurationError(String),
    DirectoryReadError(String),
    FileReadError(String),
    JsonParseError(String),
    ClassAlreadyExists(String),
    ClassNotFound(String),
    ObjectAlreadyExists(String),
    ObjectNotFound(String),
    RuleAlreadyExists(String),
    RuleNotFound(String),
    DatabaseError(String),
    KnowledgeBaseError(String),
}

impl fmt::Display for CoCoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CoCoError::ConfigurationError(msg) => write!(f, "Configuration error: {}", msg),
            CoCoError::DirectoryReadError(msg) => write!(f, "Failed to read directory: {}", msg),
            CoCoError::FileReadError(msg) => write!(f, "Failed to read file: {}", msg),
            CoCoError::JsonParseError(msg) => write!(f, "Failed to parse JSON: {}", msg),
            CoCoError::ClassAlreadyExists(msg) => write!(f, "Class already exists: {}", msg),
            CoCoError::ClassNotFound(msg) => write!(f, "Class not found: {}", msg),
            CoCoError::ObjectAlreadyExists(msg) => write!(f, "Object already exists: {}", msg),
            CoCoError::ObjectNotFound(msg) => write!(f, "Object not found: {}", msg),
            CoCoError::RuleAlreadyExists(msg) => write!(f, "Rule already exists: {}", msg),
            CoCoError::RuleNotFound(msg) => write!(f, "Rule not found: {}", msg),
            CoCoError::DatabaseError(msg) => write!(f, "Database error: {}", msg),
            CoCoError::KnowledgeBaseError(msg) => write!(f, "Knowledge base error: {}", msg),
        }
    }
}
