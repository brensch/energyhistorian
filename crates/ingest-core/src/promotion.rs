use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PromotionMode {
    RawOnly,
    TypedApproved,
    BlockedPendingReview,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalDataset {
    pub dataset_id: String,
    pub description: String,
    pub grain: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromotionMapping {
    pub source_logical_table: String,
    pub source_schema_key: String,
    pub canonical_dataset: String,
    pub mode: PromotionMode,
    pub notes: Vec<String>,
}
