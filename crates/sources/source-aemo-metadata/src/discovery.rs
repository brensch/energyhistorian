use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use ingest_core::{ArtifactKind, ArtifactMetadata, DiscoveredArtifact, RunContext};
use regex::Regex;

use crate::mms_parser;

pub const DATA_MODEL_INDEX_URL: &str =
    "https://di-help.docs.public.aemo.com.au/Content/Data_Model/MMS_Data_Model.htm?TocPath=_____8";
pub const REPORT_RELATIONSHIPS_URL: &str =
    "https://di-help.docs.public.aemo.com.au/Content/Data_Subscription/TableToFileReport.htm";
pub const POPULATION_DATES_URL: &str = "https://tech-specs.docs.public.aemo.com.au/Content/TSP_TechnicalSpecificationPortal/Data_population_dates.htm";

#[derive(Debug, Clone)]
pub struct CurrentModelRelease {
    pub model_version: String,
    pub release_name: String,
}

pub async fn discover_collection(
    client: &reqwest::Client,
    collection_id: &str,
    ctx: &RunContext,
) -> Result<Vec<DiscoveredArtifact>> {
    match collection_id {
        "current-data-model" => discover_current_data_model(client, ctx).await,
        "report-relationships" => Ok(vec![daily_snapshot_artifact(
            collection_id,
            REPORT_RELATIONSHIPS_URL,
            ArtifactKind::HtmlDocument,
            ctx,
        )]),
        "population-dates" => Ok(vec![daily_snapshot_artifact(
            collection_id,
            POPULATION_DATES_URL,
            ArtifactKind::HtmlDocument,
            ctx,
        )]),
        other => Err(anyhow!("unknown html metadata collection '{other}'")),
    }
}

pub async fn discover_current_data_model(
    client: &reqwest::Client,
    ctx: &RunContext,
) -> Result<Vec<DiscoveredArtifact>> {
    let index_html = client
        .get(DATA_MODEL_INDEX_URL)
        .send()
        .await?
        .text()
        .await
        .context("fetching AEMO data model index")?;

    let current_release = extract_current_release(&index_html)
        .context("inferring current Electricity Data Model version from AEMO index")?;
    Ok(vec![
        daily_snapshot_artifact(
            "current-data-model",
            DATA_MODEL_INDEX_URL,
            ArtifactKind::HtmlDocument,
            ctx,
        ),
        DiscoveredArtifact {
            metadata: ArtifactMetadata {
                artifact_id: format!(
                    "aemo_metadata_html:current-data-model:model-html:v{}",
                    sanitize_part(&current_release.model_version)
                ),
                source_id: "aemo_metadata_html".to_string(),
                acquisition_uri: mms_parser::TOC_URL.to_string(),
                discovered_at: Utc::now(),
                fetched_at: None,
                published_at: None,
                content_sha256: None,
                content_length_bytes: None,
                kind: ArtifactKind::HtmlDocument,
                parser_version: ctx.parser_version.clone(),
                model_version: Some(current_release.model_version.clone()),
                release_name: Some(current_release.release_name.clone()),
            },
        },
    ])
}

pub fn daily_snapshot_artifact(
    collection_id: &str,
    url: &str,
    kind: ArtifactKind,
    ctx: &RunContext,
) -> DiscoveredArtifact {
    let snapshot_date = Utc::now().format("%Y-%m-%d");
    DiscoveredArtifact {
        metadata: ArtifactMetadata {
            artifact_id: format!(
                "aemo_metadata_html:{collection_id}:snapshot:{snapshot_date}:{}",
                sanitize_part(url.rsplit('/').next().unwrap_or(collection_id))
            ),
            source_id: "aemo_metadata_html".to_string(),
            acquisition_uri: url.to_string(),
            discovered_at: Utc::now(),
            fetched_at: None,
            published_at: None,
            content_sha256: None,
            content_length_bytes: None,
            kind,
            parser_version: ctx.parser_version.clone(),
            model_version: None,
            release_name: None,
        },
    }
}

fn extract_current_release(html: &str) -> Result<CurrentModelRelease> {
    let re = Regex::new(r#"Electricity_Data_Model_Report_(\d+)\.pdf"#)?;
    let version_digits = re
        .captures(html)
        .and_then(|caps| caps.get(1).map(|m| m.as_str().to_string()))
        .ok_or_else(|| anyhow!("could not find current Electricity Data Model PDF link"))?;
    let model_version = digits_to_version(&version_digits);

    Ok(CurrentModelRelease {
        release_name: format!("Electricity Data Model v{model_version}"),
        model_version,
    })
}

fn digits_to_version(input: &str) -> String {
    if input.len() <= 1 {
        input.to_string()
    } else {
        let (major, minor) = input.split_at(input.len() - 1);
        format!("{major}.{minor}")
    }
}

pub fn sanitize_part(value: &str) -> String {
    value
        .to_ascii_lowercase()
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}
