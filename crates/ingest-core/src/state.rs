use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ArtifactProcessingStatus {
    Discovered,
    Fetched,
    Parsed,
    Promoted,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingCheckpoint {
    pub source_id: String,
    pub cursor: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingEvent {
    pub artifact_id: String,
    pub source_id: String,
    pub status: ArtifactProcessingStatus,
    pub occurred_at: DateTime<Utc>,
    pub message: String,
}
