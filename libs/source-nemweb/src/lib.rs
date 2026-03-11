pub mod discover;
pub mod families;
pub mod fetch;
pub mod ingest;
pub mod parse;

use std::path::Path;

use anyhow::{Result, anyhow, bail};
use chrono::Utc;
use ingest_core::{
    ArtifactKind, ArtifactMetadata, BoxedFuture, CollectionCompletion, CompletionUnit,
    DiscoveredArtifact, DiscoveryRequest, LocalArtifact, ParseResult, PluginCapabilities,
    PromotionSpec, RawTableRowSink, RunContext, RuntimePluginParseResult, RuntimeSourcePlugin,
    SourceCollection, SourceDescriptor, SourceFamilyCatalogEntry, SourceMetadataDocument,
    SourcePlugin, TaskBlueprint, TaskKind,
};

pub use ingest::{ArchiveManifest, NemwebIngestResult, ParsedTableBatch};

#[derive(Clone)]
pub struct NemwebPlugin;

impl Default for NemwebPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl NemwebPlugin {
    pub fn new() -> Self {
        Self
    }

    pub fn catalog(&self) -> Vec<SourceFamilyCatalogEntry> {
        families::catalog_entries()
    }

    /// Discover recent archives for a NEMweb source family/collection.
    pub async fn discover_collection(
        &self,
        client: &reqwest::Client,
        collection_id: &str,
        limit: usize,
    ) -> Result<Vec<DiscoveredArtifact>> {
        let family = families::lookup_family(collection_id)?;
        let ctx = RunContext {
            run_id: format!("{}-{}", family.id, Utc::now().timestamp_millis()),
            environment: "service".to_string(),
            parser_version: "source-nemweb/0.1".to_string(),
        };
        discover::discover_recent_archives(client, &family, limit, &ctx).await
    }

    /// Fetch a single discovered artifact archive.
    pub async fn fetch_artifact(
        &self,
        client: &reqwest::Client,
        artifact: &DiscoveredArtifact,
        output_dir: &Path,
    ) -> Result<LocalArtifact> {
        fetch::fetch_archive(client, artifact, output_dir).await
    }

    /// Parse a locally stored archive into schemas and row outputs.
    pub fn parse_artifact(
        &self,
        artifact: &LocalArtifact,
        parsed_dir: &Path,
    ) -> Result<ParseResult> {
        parse::parse_local_archive(artifact, parsed_dir)
    }

    pub async fn ingest_recent(
        &self,
        source_family_id: &str,
        limit: usize,
        raw_dir: &Path,
        parsed_dir: &Path,
    ) -> Result<NemwebIngestResult> {
        ingest::ingest_recent(source_family_id, limit, raw_dir, parsed_dir).await
    }
}

impl SourcePlugin for NemwebPlugin {
    fn descriptor(&self) -> SourceDescriptor {
        SourceDescriptor {
            source_id: "aemo.nemweb".to_string(),
            domain: "electricity".to_string(),
            description: "AEMO NEMweb current/archive market report families.".to_string(),
            versioned_metadata: true,
            historical_backfill_supported: true,
        }
    }

    fn capabilities(&self) -> PluginCapabilities {
        PluginCapabilities {
            supports_backfill: true,
            supports_schema_registry: true,
            supports_historical_media: false,
            notes: vec![
                "One archive can emit many logical tables.".to_string(),
                "Schemas are defined by I rows and must be version-tracked.".to_string(),
            ],
        }
    }

    fn collections(&self) -> Vec<SourceCollection> {
        self.catalog()
            .into_iter()
            .map(|family| SourceCollection {
                id: family.id.clone(),
                display_name: family.id.to_uppercase(),
                description: family.description,
                retrieval_modes: vec![
                    "discover-current".to_string(),
                    "fetch-archive".to_string(),
                    "parse-cid-csv".to_string(),
                ],
                completion: CollectionCompletion {
                    unit: CompletionUnit::Artifact,
                    dedupe_keys: vec![
                        "remote_url".to_string(),
                        "published_at".to_string(),
                        "content_sha256".to_string(),
                    ],
                    cursor_field: Some("published_at".to_string()),
                    mutable_window_seconds: Some(86_400),
                    notes: vec![
                        "Completion is tracked per downloaded archive.".to_string(),
                        "Archives are mutable for a short window because AEMO can republish files."
                            .to_string(),
                    ],
                },
                task_blueprints: vec![
                    TaskBlueprint {
                        kind: TaskKind::Discover,
                        description: "Poll NEMweb directory listings for candidate archives."
                            .to_string(),
                        max_concurrency: 1,
                        queue: "discover".to_string(),
                        idempotency_scope: "source+collection+cursor".to_string(),
                    },
                    TaskBlueprint {
                        kind: TaskKind::Fetch,
                        description: "Download new or changed NEMweb archives.".to_string(),
                        max_concurrency: 2,
                        queue: "fetch".to_string(),
                        idempotency_scope: "artifact_id".to_string(),
                    },
                    TaskBlueprint {
                        kind: TaskKind::Parse,
                        description: "Extract CID tables and raw logical outputs from archives."
                            .to_string(),
                        max_concurrency: 4,
                        queue: "parse".to_string(),
                        idempotency_scope: "artifact_id+parser_version".to_string(),
                    },
                    TaskBlueprint {
                        kind: TaskKind::RegisterSchema,
                        description: "Register schema observations from I records.".to_string(),
                        max_concurrency: 1,
                        queue: "schema".to_string(),
                        idempotency_scope: "schema_id".to_string(),
                    },
                    TaskBlueprint {
                        kind: TaskKind::ReconcileRawStorage,
                        description: "Create raw schema-hash-specific storage tables as needed."
                            .to_string(),
                        max_concurrency: 1,
                        queue: "reconcile".to_string(),
                        idempotency_scope: "logical_table+header_hash".to_string(),
                    },
                ],
                default_poll_interval_seconds: Some(900),
            })
            .collect()
    }

    fn metadata_catalog(&self) -> Vec<SourceMetadataDocument> {
        Vec::new()
    }

    fn discover(
        &self,
        request: &DiscoveryRequest,
        ctx: &RunContext,
    ) -> Result<Vec<DiscoveredArtifact>> {
        let family_id = request
            .collection
            .as_deref()
            .ok_or_else(|| anyhow!("nemweb discovery requires a collection/source family"))?;
        let family = families::lookup_family(family_id)?;
        Ok((0..request.limit.unwrap_or(4))
            .map(|idx| DiscoveredArtifact {
                metadata: ArtifactMetadata {
                    artifact_id: format!("{}-{}-{}", family.id, ctx.run_id, idx),
                    source_id: family.id.to_string(),
                    acquisition_uri: family.listing_url.to_string(),
                    discovered_at: Utc::now(),
                    fetched_at: None,
                    published_at: None,
                    content_sha256: None,
                    content_length_bytes: None,
                    kind: ArtifactKind::ZipArchive,
                    parser_version: ctx.parser_version.clone(),
                    model_version: None,
                    release_name: None,
                },
            })
            .collect())
    }

    fn fetch(&self, _artifact: &DiscoveredArtifact, _ctx: &RunContext) -> Result<LocalArtifact> {
        bail!("Use ingest_recent or family-specific fetch for NEMweb")
    }

    fn inspect_parse(&self, artifact: &LocalArtifact, _ctx: &RunContext) -> Result<ParseResult> {
        let plan = parse::inspect_local_archive(artifact)?;
        Ok(ParseResult {
            observed_schemas: plan.observed_schemas,
            raw_outputs: plan.raw_outputs,
            promotions: Vec::new(),
        })
    }

    fn stream_parse(
        &self,
        artifact: &LocalArtifact,
        _ctx: &RunContext,
        sink: &mut dyn RawTableRowSink,
    ) -> Result<()> {
        let plan = parse::inspect_local_archive(artifact)?;
        parse::stream_local_archive_rows(artifact, &plan, sink)
    }

    fn promotion_plan(&self) -> &'static [PromotionSpec] {
        // Maps source-specific logical tables to canonical dataset names.
        // This defines how raw ingested data gets promoted to the stable
        // semantic layer once semantic view reconciliation is implemented.
        const PROMOTIONS: &[PromotionSpec] = &[
            PromotionSpec {
                source_logical_table: "TRADINGIS/TRADING/PRICE/3",
                canonical_dataset: "canonical.trading_price",
                mapping_name: "nemweb_trading_price_v3",
            },
            PromotionSpec {
                source_logical_table: "TRADINGIS/TRADING/INTERCONNECTORRES/2",
                canonical_dataset: "canonical.trading_interconnectorres",
                mapping_name: "nemweb_trading_interconnectorres_v2",
            },
            PromotionSpec {
                source_logical_table: "DISPATCHIS/DISPATCH/LOCAL_PRICE/1",
                canonical_dataset: "canonical.dispatch_local_price",
                mapping_name: "nemweb_dispatch_local_price_v1",
            },
            PromotionSpec {
                source_logical_table: "DISPATCHIS/DISPATCH/CASE_SOLUTION/2",
                canonical_dataset: "canonical.dispatch_case_solution",
                mapping_name: "nemweb_dispatch_case_solution_v2",
            },
        ];
        PROMOTIONS
    }
}

impl RuntimeSourcePlugin for NemwebPlugin {
    fn parser_version(&self) -> &'static str {
        "source-nemweb/0.1"
    }

    fn discover_collection_async<'a>(
        &'a self,
        client: &'a reqwest::Client,
        collection_id: &'a str,
        limit: usize,
        _ctx: &'a RunContext,
    ) -> BoxedFuture<'a, Result<Vec<DiscoveredArtifact>>> {
        Box::pin(async move { self.discover_collection(client, collection_id, limit).await })
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
        ctx: &RunContext,
    ) -> Result<RuntimePluginParseResult> {
        let result = self.inspect_parse(&artifact, ctx)?;
        Ok(RuntimePluginParseResult::StructuredRaw { artifact, result })
    }

    fn stream_structured_parse_runtime(
        &self,
        artifact: &LocalArtifact,
        _collection_id: &str,
        ctx: &RunContext,
        sink: &mut dyn RawTableRowSink,
    ) -> Result<()> {
        self.stream_parse(artifact, ctx, sink)
    }
}
