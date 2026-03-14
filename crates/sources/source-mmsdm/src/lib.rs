// ── source-mmsdm ─────────────────────────────────────────────────────────
//
// Ingestion plugin for AEMO's MMSDM (Market Management System Data Model)
// monthly archive.  MMSDM publishes ~200 tables per month as individual zip
// files at:
//
//   https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/
//
// Directory structure:
//   {root}/{year}/MMSDM_{year}_{month}/MMSDM_Historical_Data_SQLLoader/DATA/
//     PUBLIC_ARCHIVE#TABLE_NAME#FILE01#YYYYMM010000.zip
//
// We don't capture all ~200 tables — only the ones useful for energy market
// analysis.  See tables.rs for the full list and rationale.
//
// Module layout:
//   lib.rs      — Plugin trait implementations and fetch/parse wiring
//   tables.rs   — Which tables we capture, with descriptions
//   discover.rs — Two-phase skeleton+batch discovery logic
//   semantic.rs — ClickHouse semantic views and model registry metadata

mod discover;
mod semantic;
pub mod tables;

use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use ingest_core::{
    BoxedFuture, CollectionCompletion, CompletionUnit, DiscoveredArtifact, DiscoveryCursorHint,
    DiscoveryRequest, LocalArtifact, ParseResult, PluginCapabilities, PromotionSpec,
    RawTableRowSink, RunContext, RuntimePluginParseResult, RuntimeSourcePlugin, SemanticJob,
    SourceCollection, SourceDescriptor, SourceMetadataDocument, SourcePlugin,
    StructuredRawEventSink, TaskBlueprint, TaskKind,
};
use sha2::{Digest, Sha256};

use discover::{COLLECTION_ID, SOURCE_ID};

#[derive(Clone)]
pub struct MmsdmPlugin;

impl MmsdmPlugin {
    pub fn new() -> Self {
        Self
    }

    /// Download a discovered artifact zip to local disk.
    ///
    /// Validates that the response is actually a zip file (not an HTML error
    /// page) before writing it.
    pub async fn fetch_artifact(
        &self,
        client: &reqwest::Client,
        artifact: &DiscoveredArtifact,
        output_dir: &Path,
    ) -> Result<LocalArtifact> {
        fs::create_dir_all(output_dir)?;
        let response = client
            .get(&artifact.metadata.acquisition_uri)
            .send()
            .await
            .with_context(|| format!("downloading {}", artifact.metadata.acquisition_uri))?;
        let bytes = response
            .error_for_status()
            .with_context(|| format!("downloading {}", artifact.metadata.acquisition_uri))?
            .bytes()
            .await?
            .to_vec();

        validate_zip_payload(&artifact.metadata.acquisition_uri, &bytes)?;

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

    /// Parse a downloaded MMSDM zip into observed schemas.
    ///
    /// Delegates to source-nemweb's generic CID CSV parser — MMSDM and
    /// NEMweb zips use the same CSV format.
    pub fn parse_artifact(&self, artifact: &LocalArtifact) -> Result<ParseResult> {
        source_nemweb::parse::parse_local_archive(artifact, Path::new("."))
    }

    /// Stream parsed rows from a downloaded MMSDM zip into a row sink.
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

// ── SourcePlugin (sync trait) ────────────────────────────────────────────
//
// This trait defines the plugin's metadata (descriptor, capabilities,
// collections) and synchronous parse methods.  The async discovery and
// fetch are on RuntimeSourcePlugin below.

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
                "Discovers per-table zips from the public MMSDM monthly DATA directories.".into(),
                format!(
                    "Captures {} tables across registration, dispatch, bidding, and more.",
                    tables::include_tables().len()
                ),
            ],
        }
    }

    fn collections(&self) -> Vec<SourceCollection> {
        vec![SourceCollection {
            id: COLLECTION_ID.to_string(),
            display_name: "Public Reference Data".to_string(),
            description: "Public MMSDM monthly table zips covering registration, \
                dispatch, bidding, pricing, demand, constraints, and more."
                .to_string(),
            retrieval_modes: vec![
                "discover-archive".into(),
                "fetch-zip".into(),
                "parse-cid-csv".into(),
            ],
            completion: CollectionCompletion {
                unit: CompletionUnit::Artifact,
                dedupe_keys: vec![
                    "artifact_id".into(),
                    "remote_url".into(),
                    "content_sha256".into(),
                ],
                cursor_field: Some("published_at".into()),
                mutable_window_seconds: Some(86_400),
                notes: vec![
                    "Fetches per-table zips from MMSDM monthly DATA directories.".into(),
                    "Discovery uses a two-phase skeleton+batch approach.".into(),
                ],
            },
            task_blueprints: vec![
                TaskBlueprint {
                    kind: TaskKind::Discover,
                    description: "Discover monthly public MMSDM table zips.".into(),
                    max_concurrency: 1,
                    queue: "discover".into(),
                    idempotency_scope: "source+collection+artifact".into(),
                },
                TaskBlueprint {
                    kind: TaskKind::Fetch,
                    description: "Download direct MMSDM table zips.".into(),
                    max_concurrency: 2,
                    queue: "fetch".into(),
                    idempotency_scope: "artifact_id".into(),
                },
                TaskBlueprint {
                    kind: TaskKind::Parse,
                    description: "Parse MMSDM CID CSV zips into raw tables.".into(),
                    max_concurrency: 4,
                    queue: "parse".into(),
                    idempotency_scope: "artifact_id+parser_version".into(),
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
        bail!("Use discover_collection_async() for async MMSDM discovery")
    }

    fn fetch(&self, _artifact: &DiscoveredArtifact, _ctx: &RunContext) -> Result<LocalArtifact> {
        bail!("Use fetch_artifact_async() for async MMSDM fetching")
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

    fn semantic_jobs(&self) -> Vec<SemanticJob> {
        semantic::semantic_jobs()
    }
}

// ── RuntimeSourcePlugin (async trait) ────────────────────────────────────
//
// The runtime trait adds async discovery and fetch methods that use
// reqwest::Client for HTTP.  These are called by the orchestrator.

impl RuntimeSourcePlugin for MmsdmPlugin {
    fn parser_version(&self) -> &'static str {
        "source-mmsdm/0.1"
    }

    fn discover_collection_async<'a>(
        &'a self,
        client: &'a reqwest::Client,
        collection_id: &'a str,
        limit: usize,
        cursor: &'a DiscoveryCursorHint,
        ctx: &'a RunContext,
    ) -> BoxedFuture<'a, Result<Vec<DiscoveredArtifact>>> {
        Box::pin(async move {
            if collection_id != COLLECTION_ID {
                bail!("unknown MMSDM collection '{collection_id}'");
            }
            discover::discover_public_reference_data(client, limit, cursor, ctx).await
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
        Ok(RuntimePluginParseResult::StructuredRaw { artifact })
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

    fn stream_structured_parse_events_runtime(
        &self,
        artifact: &LocalArtifact,
        _collection_id: &str,
        _ctx: &RunContext,
        sink: &mut dyn StructuredRawEventSink,
    ) -> Result<()> {
        source_nemweb::parse::stream_local_archive_events(artifact, sink)
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────

/// Validate that a downloaded payload is actually a zip file, not an HTML
/// error page or other unexpected content.
fn validate_zip_payload(url: &str, bytes: &[u8]) -> Result<()> {
    const ZIP_PREFIXES: [&[u8]; 3] = [b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"];
    if ZIP_PREFIXES.iter().any(|prefix| bytes.starts_with(prefix)) {
        return Ok(());
    }
    let preview = String::from_utf8_lossy(&bytes[..bytes.len().min(160)]);
    bail!("downloaded non-zip payload from {url}: {preview}");
}
