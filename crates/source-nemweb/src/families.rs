use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct SourceFamily {
    pub id: &'static str,
    pub description: &'static str,
    pub current_reports_url: &'static str,
    pub cadence: &'static str,
    pub recommendation_rank: u8,
    pub recommendation_reason: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceFamilyCatalogEntry {
    pub id: String,
    pub description: String,
    pub current_reports_url: String,
    pub cadence: String,
    pub recommendation_rank: u8,
    pub recommendation_reason: String,
}

const SOURCE_FAMILIES: &[SourceFamily] = &[
    SourceFamily {
        id: "tradingis",
        description: "5-minute trading interval regional prices and interconnector results.",
        current_reports_url: "https://nemweb.com.au/Reports/Current/TradingIS_Reports/",
        cadence: "every 5 minutes",
        recommendation_rank: 1,
        recommendation_reason: "Best first slice for portal value: regional prices and interconnector flow fit common market questions with a compact schema.",
    },
    SourceFamily {
        id: "dispatchis",
        description: "Dispatch interval outcomes including local prices and case solution data.",
        current_reports_url: "https://nemweb.com.au/Reports/Current/DispatchIS_Reports/",
        cadence: "every 5 minutes",
        recommendation_rank: 2,
        recommendation_reason: "Useful second slice once regional price questions work. Adds local price adjustment and dispatch-run context.",
    },
];

pub fn source_families() -> &'static [SourceFamily] {
    SOURCE_FAMILIES
}

pub fn lookup_family(source_family_id: &str) -> Result<&'static SourceFamily> {
    source_families()
        .iter()
        .find(|family| family.id == source_family_id)
        .ok_or_else(|| anyhow!("unknown source family `{source_family_id}`"))
}

pub fn catalog_entries() -> Vec<SourceFamilyCatalogEntry> {
    source_families()
        .iter()
        .map(|family| SourceFamilyCatalogEntry {
            id: family.id.to_string(),
            description: family.description.to_string(),
            current_reports_url: family.current_reports_url.to_string(),
            cadence: family.cadence.to_string(),
            recommendation_rank: family.recommendation_rank,
            recommendation_reason: family.recommendation_reason.to_string(),
        })
        .collect()
}
