use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub type SchemaObservationId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalTableId {
    pub source_family: String,
    pub section: String,
    pub table: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersionKey {
    pub logical_table: LogicalTableId,
    pub report_version: String,
    pub model_version: Option<String>,
    pub header_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaColumn {
    pub ordinal: usize,
    pub name: String,
    pub source_data_type: Option<String>,
    pub nullable: Option<bool>,
    pub primary_key: Option<bool>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemaApprovalStatus {
    Proposed,
    Approved,
    Deprecated,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservedSchema {
    pub schema_id: SchemaObservationId,
    pub schema_key: SchemaVersionKey,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub observed_in_artifact_id: String,
    pub columns: Vec<SchemaColumn>,
    pub approval_status: SchemaApprovalStatus,
}
