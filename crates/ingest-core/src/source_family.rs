use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceFamily {
    pub id: String,
    pub description: String,
    pub listing_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceFamilyCatalogEntry {
    pub id: String,
    pub description: String,
    pub listing_url: String,
}
