use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub type ArtifactId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ArtifactKind {
    ZipArchive,
    CsvFile,
    PdfDocument,
    HtmlDocument,
    CsvExport,
    DvdImage,
    ExtractedMedia,
    ApiResponse,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactMetadata {
    pub artifact_id: ArtifactId,
    pub source_id: String,
    pub acquisition_uri: String,
    pub discovered_at: DateTime<Utc>,
    pub fetched_at: Option<DateTime<Utc>>,
    pub published_at: Option<DateTime<Utc>>,
    pub content_sha256: Option<String>,
    pub content_length_bytes: Option<u64>,
    pub kind: ArtifactKind,
    pub parser_version: String,
    pub model_version: Option<String>,
    pub release_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredArtifact {
    pub metadata: ArtifactMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalArtifact {
    pub metadata: ArtifactMetadata,
    pub local_path: PathBuf,
}
