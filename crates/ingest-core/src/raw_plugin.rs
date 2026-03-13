use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawPluginTableBatch {
    pub table_name: String,
    pub rows: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawPluginParseResult {
    pub tables: Vec<RawPluginTableBatch>,
}
