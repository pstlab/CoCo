pub mod clips_kb;
#[cfg(feature = "ollama")]
pub mod ollama;

pub use clips_kb::CLIPSKnowledgeBase;
pub mod clips;
