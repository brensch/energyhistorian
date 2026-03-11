use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use ingest_core::{
    ArtifactKind, ArtifactMetadata, BoxedFuture, CollectionCompletion, CompletionUnit,
    DiscoveredArtifact, DiscoveryCursorHint, DiscoveryRequest, LocalArtifact, ParseResult,
    PluginCapabilities, PromotionSpec, RawTableRowSink, RunContext, RuntimePluginParseResult,
    RuntimeSourcePlugin, SourceCollection, SourceDescriptor, SourceMetadataDocument, SourcePlugin,
    TaskBlueprint, TaskKind,
};
use regex::Regex;
use sha2::{Digest, Sha256};

const MMSDM_ARCHIVE_ROOT: &str = "https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/";
const NEMWEB_ROOT: &str = "https://nemweb.com.au";
const SOURCE_ID: &str = "aemo.mmsdm";
const COLLECTION_ID: &str = "public-reference-data";
const INCLUDE_TABLES: &[&str] = &[
    "DUALLOC",
    "DUDETAIL",
    "DUDETAILSUMMARY",
    "GENUNITS",
    "STADUALLOC",
    "STATION",
    "STATIONOWNER",
    "STATIONOPERATINGSTATUS",
];

#[derive(Clone)]
pub struct MmsdmPlugin;

impl MmsdmPlugin {
    pub fn new() -> Self {
        Self
    }

    pub async fn discover_collection(
        &self,
        client: &reqwest::Client,
        collection_id: &str,
        cursor: &DiscoveryCursorHint,
        ctx: &RunContext,
    ) -> Result<Vec<DiscoveredArtifact>> {
        if collection_id != COLLECTION_ID {
            bail!("unknown MMSDM collection '{collection_id}'");
        }
        discover_public_reference_data(client, cursor, ctx).await
    }

    pub async fn fetch_artifact(
        &self,
        client: &reqwest::Client,
        artifact: &DiscoveredArtifact,
        output_dir: &Path,
    ) -> Result<LocalArtifact> {
        fs::create_dir_all(output_dir)?;
        let bytes = client
            .get(&artifact.metadata.acquisition_uri)
            .send()
            .await
            .with_context(|| format!("downloading {}", artifact.metadata.acquisition_uri))?
            .bytes()
            .await?
            .to_vec();
        let filename = artifact
            .metadata
            .acquisition_uri
            .rsplit('/')
            .next()
            .unwrap_or("artifact.zip");
        let local_path = output_dir.join(filename);
        fs::write(&local_path, &bytes)?;

        let mut metadata = artifact.metadata.clone();
        metadata.fetched_at = Some(Utc::now());
        metadata.content_sha256 = Some(format!("{:x}", Sha256::digest(&bytes)));
        metadata.content_length_bytes = Some(bytes.len() as u64);

        Ok(LocalArtifact {
            metadata,
            local_path,
        })
    }

    pub fn parse_artifact(&self, artifact: &LocalArtifact) -> Result<ParseResult> {
        source_nemweb::parse::parse_local_archive(artifact, Path::new("."))
    }

    pub fn stream_artifact(
        &self,
        artifact: &LocalArtifact,
        sink: &mut dyn RawTableRowSink,
    ) -> Result<()> {
        let plan = source_nemweb::parse::inspect_local_archive(artifact)?;
        source_nemweb::parse::stream_local_archive_rows(artifact, &plan, sink)
    }
}

impl Default for MmsdmPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl SourcePlugin for MmsdmPlugin {
    fn descriptor(&self) -> SourceDescriptor {
        SourceDescriptor {
            source_id: SOURCE_ID.to_string(),
            domain: "electricity".to_string(),
            description: "AEMO MMSDM public monthly archive table zips.".to_string(),
            versioned_metadata: true,
            historical_backfill_supported: true,
        }
    }

    fn capabilities(&self) -> PluginCapabilities {
        PluginCapabilities {
            supports_backfill: true,
            supports_schema_registry: true,
            supports_historical_media: true,
            notes: vec![
                "Discovers direct per-table zips from the public MMSDM monthly DATA directories."
                    .to_string(),
                "Targets public reference tables needed for unit, station, ownership, and fuel semantics."
                    .to_string(),
            ],
        }
    }

    fn collections(&self) -> Vec<SourceCollection> {
        vec![SourceCollection {
            id: COLLECTION_ID.to_string(),
            display_name: "Public Reference Data".to_string(),
            description: "Public MMSDM monthly table zips for participant registration and unit reference data.".to_string(),
            retrieval_modes: vec!["discover-archive".to_string(), "fetch-zip".to_string(), "parse-cid-csv".to_string()],
            completion: CollectionCompletion {
                unit: CompletionUnit::Artifact,
                dedupe_keys: vec![
                    "artifact_id".to_string(),
                    "remote_url".to_string(),
                    "content_sha256".to_string(),
                ],
                cursor_field: Some("published_at".to_string()),
                mutable_window_seconds: Some(86_400),
                notes: vec![
                    "Fetches the broken-out direct table zips from MMSDM monthly DATA directories.".to_string(),
                    "Includes only public reference tables needed for query semantics and unit dimensions.".to_string(),
                ],
            },
            task_blueprints: vec![
                TaskBlueprint {
                    kind: TaskKind::Discover,
                    description: "Discover monthly public MMSDM table zips.".to_string(),
                    max_concurrency: 1,
                    queue: "discover".to_string(),
                    idempotency_scope: "source+collection+artifact".to_string(),
                },
                TaskBlueprint {
                    kind: TaskKind::Fetch,
                    description: "Download direct MMSDM table zips.".to_string(),
                    max_concurrency: 2,
                    queue: "fetch".to_string(),
                    idempotency_scope: "artifact_id".to_string(),
                },
                TaskBlueprint {
                    kind: TaskKind::Parse,
                    description: "Parse MMSDM CID CSV zips into schema-hash raw tables.".to_string(),
                    max_concurrency: 4,
                    queue: "parse".to_string(),
                    idempotency_scope: "artifact_id+parser_version".to_string(),
                },
            ],
            default_poll_interval_seconds: Some(86_400),
        }]
    }

    fn metadata_catalog(&self) -> Vec<SourceMetadataDocument> {
        Vec::new()
    }

    fn discover(
        &self,
        _request: &DiscoveryRequest,
        _ctx: &RunContext,
    ) -> Result<Vec<DiscoveredArtifact>> {
        bail!("Use discover_collection() for async MMSDM discovery")
    }

    fn fetch(&self, _artifact: &DiscoveredArtifact, _ctx: &RunContext) -> Result<LocalArtifact> {
        bail!("Use fetch_artifact() for async MMSDM fetching")
    }

    fn inspect_parse(&self, artifact: &LocalArtifact, _ctx: &RunContext) -> Result<ParseResult> {
        self.parse_artifact(artifact)
    }

    fn stream_parse(
        &self,
        artifact: &LocalArtifact,
        _ctx: &RunContext,
        sink: &mut dyn RawTableRowSink,
    ) -> Result<()> {
        self.stream_artifact(artifact, sink)
    }

    fn promotion_plan(&self) -> &'static [PromotionSpec] {
        &[]
    }
}

impl RuntimeSourcePlugin for MmsdmPlugin {
    fn parser_version(&self) -> &'static str {
        "source-mmsdm/0.1"
    }

    fn discover_collection_async<'a>(
        &'a self,
        client: &'a reqwest::Client,
        collection_id: &'a str,
        _limit: usize,
        cursor: &'a DiscoveryCursorHint,
        ctx: &'a RunContext,
    ) -> BoxedFuture<'a, Result<Vec<DiscoveredArtifact>>> {
        Box::pin(async move {
            self.discover_collection(client, collection_id, cursor, ctx)
                .await
        })
    }

    fn fetch_artifact_async<'a>(
        &'a self,
        client: &'a reqwest::Client,
        _collection_id: &'a str,
        artifact: &'a DiscoveredArtifact,
        output_dir: &'a Path,
    ) -> BoxedFuture<'a, Result<LocalArtifact>> {
        Box::pin(async move { self.fetch_artifact(client, artifact, output_dir).await })
    }

    fn parse_artifact_runtime(
        &self,
        _collection_id: &str,
        artifact: LocalArtifact,
        _ctx: &RunContext,
    ) -> Result<RuntimePluginParseResult> {
        let result = self.parse_artifact(&artifact)?;
        Ok(RuntimePluginParseResult::StructuredRaw { artifact, result })
    }

    fn stream_structured_parse_runtime(
        &self,
        artifact: &LocalArtifact,
        _collection_id: &str,
        _ctx: &RunContext,
        sink: &mut dyn RawTableRowSink,
    ) -> Result<()> {
        self.stream_artifact(artifact, sink)
    }
}

async fn discover_public_reference_data(
    client: &reqwest::Client,
    cursor: &DiscoveryCursorHint,
    ctx: &RunContext,
) -> Result<Vec<DiscoveredArtifact>> {
    let year_re = Regex::new(r#"HREF="(\d{4})/""#)?;
    let month_re = Regex::new(r#"(MMSDM_(\d{4})_(\d{2}))/"#)?;
    let href_re = Regex::new(r#"HREF="([^"]+)""#)?;
    let zip_re = Regex::new(r#"PUBLIC_ARCHIVE#([A-Z0-9_]+)#FILE(\d+)#([0-9]{6,12})\.zip"#)?;

    let root_html = fetch_text(client, MMSDM_ARCHIVE_ROOT).await?;
    let mut years = year_re
        .captures_iter(&root_html)
        .filter_map(|captures| captures.get(1).map(|m| m.as_str().to_string()))
        .collect::<Vec<_>>();
    years.sort();
    years.dedup();

    let earliest_month = cursor
        .latest_release_name
        .as_deref()
        .and_then(parse_release_month);

    let mut artifacts = Vec::new();
    for year in years {
        artifacts.extend(
            discover_year_artifacts(
                client,
                &year,
                earliest_month,
                ctx,
                &month_re,
                &href_re,
                &zip_re,
            )
            .await?,
        );
    }
    artifacts.sort_by(|left, right| left.metadata.artifact_id.cmp(&right.metadata.artifact_id));

    Ok(artifacts)
}

async fn discover_year_artifacts(
    client: &reqwest::Client,
    year: &str,
    earliest_month: Option<(i32, u32)>,
    ctx: &RunContext,
    month_re: &Regex,
    href_re: &Regex,
    zip_re: &Regex,
) -> Result<Vec<DiscoveredArtifact>> {
    let year_url = format!("{MMSDM_ARCHIVE_ROOT}{year}/");
    let year_html = fetch_text(client, &year_url).await?;
    let mut months = month_re
        .captures_iter(&year_html)
        .filter_map(|captures| {
            Some((
                captures.get(1)?.as_str().to_string(),
                format!(
                    "{}-{}",
                    captures.get(2)?.as_str(),
                    captures.get(3)?.as_str()
                ),
                captures.get(2)?.as_str().parse::<i32>().ok()?,
                captures.get(3)?.as_str().parse::<u32>().ok()?,
            ))
        })
        .collect::<Vec<_>>();
    months.sort();
    months.dedup();

    let mut artifacts = Vec::new();
    for (month_dir, month_key, month_year, month_number) in months {
        if let Some((cursor_year, cursor_month)) = earliest_month {
            if (month_year, month_number) < (cursor_year, cursor_month) {
                continue;
            }
        }
        artifacts.extend(
            discover_month_artifacts(
                client, &year_url, &month_dir, &month_key, ctx, href_re, zip_re,
            )
            .await?,
        );
    }
    Ok(artifacts)
}

async fn discover_month_artifacts(
    client: &reqwest::Client,
    year_url: &str,
    month_dir: &str,
    month_key: &str,
    ctx: &RunContext,
    href_re: &Regex,
    zip_re: &Regex,
) -> Result<Vec<DiscoveredArtifact>> {
    let data_url = format!("{year_url}{month_dir}/MMSDM_Historical_Data_SQLLoader/DATA/");
    let data_html = fetch_text(client, &data_url).await.unwrap_or_default();
    let mut hrefs = href_re
        .captures_iter(&data_html)
        .filter_map(|captures| captures.get(1).map(|m| m.as_str().to_string()))
        .collect::<Vec<_>>();
    hrefs.sort();
    hrefs.dedup();

    let mut artifacts = Vec::new();
    for href in hrefs {
        let decoded_href = href.replace("%23", "#");
        let Some(filename) = decoded_href.rsplit('/').next() else {
            continue;
        };
        let Some(captures) = zip_re.captures(filename) else {
            continue;
        };
        let Some(table_name) = captures.get(1).map(|m| m.as_str()) else {
            continue;
        };
        if !INCLUDE_TABLES.contains(&table_name) {
            continue;
        }
        let Some(file_no) = captures.get(2).map(|m| m.as_str()) else {
            continue;
        };
        let Some(published_token) = captures.get(3).map(|m| m.as_str()) else {
            continue;
        };
        let acquisition_uri = if href.starts_with("http") {
            href.clone()
        } else if href.starts_with('/') {
            format!("{NEMWEB_ROOT}{href}")
        } else {
            format!("{data_url}{href}")
        };
        artifacts.push(DiscoveredArtifact {
            metadata: ArtifactMetadata {
                artifact_id: format!(
                    "aemo_mmsdm_{month_key}_{table_name}_file{file_no}_{published_token}"
                ),
                source_id: SOURCE_ID.to_string(),
                acquisition_uri,
                discovered_at: Utc::now(),
                fetched_at: None,
                published_at: Some(Utc::now()),
                content_sha256: None,
                content_length_bytes: None,
                kind: ArtifactKind::ZipArchive,
                parser_version: ctx.parser_version.clone(),
                model_version: None,
                release_name: Some(month_key.to_string()),
            },
        });
    }
    Ok(artifacts)
}

async fn fetch_text(client: &reqwest::Client, url: &str) -> Result<String> {
    client
        .get(url)
        .send()
        .await
        .with_context(|| format!("fetching {url}"))?
        .error_for_status()
        .with_context(|| format!("unexpected status fetching {url}"))?
        .text()
        .await
        .with_context(|| format!("reading body for {url}"))
}

fn parse_release_month(release_name: &str) -> Option<(i32, u32)> {
    let (year, month) = release_name.split_once('-')?;
    Some((year.parse().ok()?, month.parse().ok()?))
}
