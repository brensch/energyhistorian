use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceDescriptor {
    pub source_id: String,
    pub domain: String,
    pub description: String,
    pub versioned_metadata: bool,
    pub historical_backfill_supported: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PluginCatalog {
    pub sources: Vec<SourceDescriptor>,
}
