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
    DiscoveredArtifact, DiscoveryCursorHint, DiscoveryRequest, LocalArtifact, ParseResult,
    PluginCapabilities, PromotionSpec, RawTableRowSink, RunContext, RuntimePluginParseResult,
    RuntimeSourcePlugin, SemanticJob, SemanticModel, SemanticNamingStrategy, SourceCollection,
    SourceDescriptor, SourceFamilyCatalogEntry, SourceMetadataDocument, SourcePlugin,
    StructuredRawEventSink, TaskBlueprint, TaskKind, semantic_model_registry_sql,
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
        cursor: &DiscoveryCursorHint,
        ctx: &RunContext,
    ) -> Result<Vec<DiscoveredArtifact>> {
        let family = families::lookup_family(collection_id)?;
        discover::discover_recent_archives(client, &family, limit, cursor, ctx).await
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

    fn semantic_models(&self) -> Vec<SemanticModel> {
        vec![
            semantic_model(
                "aemo.nemweb",
                "semantic.dispatch_price",
                "Regional 5-minute energy and FCAS price outcomes by settlement interval.",
                "one row per settlement interval per region",
                Some("SETTLEMENTDATE"),
                &["REGIONID"],
                &[
                    "RRP",
                    "RAISE6SECRRP",
                    "RAISE60SECRRP",
                    "RAISE5MINRRP",
                    "RAISEREGRRP",
                    "LOWER6SECRRP",
                    "LOWER60SECRRP",
                    "LOWER5MINRRP",
                    "LOWERREGRRP",
                ],
                &["semantic.daily_region_dispatch on SETTLEMENTDATE, REGIONID"],
                &["Operational price outcomes, not settlement amounts."],
                &["price", "volatility", "fcas", "region", "spread"],
            ),
            semantic_model(
                "aemo.nemweb",
                "semantic.daily_region_dispatch",
                "Regional dispatch outcomes, demand, interchange, available generation, and regional FCAS enablement by settlement interval.",
                "one row per settlement interval per region",
                Some("SETTLEMENTDATE"),
                &["REGIONID"],
                &[
                    "RRP",
                    "TOTALDEMAND",
                    "DEMANDFORECAST",
                    "DISPATCHABLEGENERATION",
                    "NETINTERCHANGE",
                    "AVAILABLEGENERATION",
                    "EXCESSGENERATION",
                    "RAISE6SECDISPATCH",
                    "RAISE60SECDISPATCH",
                    "RAISE5MINDISPATCH",
                    "RAISEREGLOCALDISPATCH",
                    "LOWER6SECDISPATCH",
                    "LOWER60SECDISPATCH",
                    "LOWER5MINDISPATCH",
                    "LOWERREGLOCALDISPATCH",
                ],
                &["semantic.dispatch_price on SETTLEMENTDATE, REGIONID"],
                &["Operational regional dispatch fact, not settlement-grade billing."],
                &["demand", "dispatch", "region", "imports", "fcas", "price"],
            ),
            semantic_model(
                "aemo.nemweb",
                "semantic.daily_unit_dispatch",
                "Unit-level dispatch targets, availability, ramp rates, and FCAS enablement by settlement interval.",
                "one row per settlement interval per DUID",
                Some("SETTLEMENTDATE"),
                &["DUID"],
                &[
                    "TOTALCLEARED",
                    "INITIALMW",
                    "AVAILABILITY",
                    "RAMPUPRATE",
                    "RAMPDOWNRATE",
                    "RAISE6SEC",
                    "RAISE60SEC",
                    "RAISE5MIN",
                    "RAISEREG",
                    "LOWER6SEC",
                    "LOWER60SEC",
                    "LOWER5MIN",
                    "LOWERREG",
                ],
                &["semantic.unit_dimension on DUID"],
                &["Dispatch targets are operational outcomes, not metered generation."],
                &["duid", "dispatch", "ramp", "battery", "fcas"],
            ),
            semantic_model(
                "aemo.nemweb",
                "semantic.dispatch_unit_solution",
                "Richer unit dispatch solution surface including UIGF, storage, and semi-scheduled fields by settlement interval.",
                "one row per settlement interval per DUID",
                Some("SETTLEMENTDATE"),
                &["DUID"],
                &[
                    "TOTALCLEARED",
                    "UIGF",
                    "ENERGY_STORAGE",
                    "INITIAL_ENERGY_STORAGE",
                ],
                &["semantic.unit_dimension on DUID"],
                &[
                    "Use this when daily_unit_dispatch does not expose the required semi-scheduled or storage field.",
                ],
                &["curtailment", "uigt", "semi-scheduled", "storage"],
            ),
            semantic_model(
                "aemo.nemweb",
                "semantic.actual_gen_duid",
                "Metered actual generation or energy readings by interval and DUID.",
                "one row per interval per DUID",
                Some("INTERVAL_DATETIME"),
                &["DUID"],
                &["MWH_READING"],
                &["semantic.unit_dimension on DUID"],
                &["Best actual-generation surface for output and generation mix questions."],
                &["generation", "actual", "fuel", "duid", "mix"],
            ),
            semantic_model(
                "aemo.nemweb",
                "semantic.dispatch_unit_scada",
                "SCADA telemetry values by settlement interval and DUID.",
                "one row per settlement interval per DUID",
                Some("SETTLEMENTDATE"),
                &["DUID"],
                &["SCADAVALUE"],
                &["semantic.unit_dimension on DUID"],
                &["Telemetry surface rather than settlement or metered energy."],
                &["scada", "telemetry", "generator"],
            ),
            semantic_model(
                "aemo.nemweb",
                "semantic.dispatch_interconnectorres",
                "Interconnector dispatch results including flow, metered flow, limits, losses, and marginal value.",
                "one row per settlement interval per interconnector",
                Some("SETTLEMENTDATE"),
                &["INTERCONNECTORID"],
                &[
                    "MWFLOW",
                    "METEREDMWFLOW",
                    "MWLOSSES",
                    "EXPORTLIMIT",
                    "IMPORTLIMIT",
                    "MARGINALVALUE",
                ],
                &["semantic.dispatch_price by aligned settlement interval for spread analysis"],
                &[
                    "Interconnector-region mapping is semantic knowledge rather than explicit table columns.",
                ],
                &["interconnector", "flow", "limit", "loss", "spread"],
            ),
            semantic_model(
                "aemo.nemweb",
                "semantic.dispatch_constraint",
                "Constraint outcomes by settlement interval and constraint identifier.",
                "one row per settlement interval per constraint",
                Some("SETTLEMENTDATE"),
                &["CONSTRAINTID"],
                &["RHS", "LHS", "MARGINALVALUE", "VIOLATIONDEGREE"],
                &[],
                &[
                    "Constraint type may need to be proxied from CONSTRAINTID unless a dedicated dimension is added.",
                ],
                &["constraint", "binding", "shadow price"],
            ),
            semantic_model(
                "aemo.nemweb",
                "semantic.predispatch_region_prices",
                "Predispatch regional price forecasts.",
                "one row per forecast interval per region",
                Some("DATETIME"),
                &["REGIONID"],
                &["RRP"],
                &["semantic.dispatch_price on DATETIME = SETTLEMENTDATE and REGIONID"],
                &[
                    "Forecast horizon logic is still thin; good for point forecast versus actual, not full horizon analytics.",
                ],
                &["forecast", "predispatch", "price"],
            ),
            semantic_model(
                "aemo.nemweb",
                "semantic.p5min_regionsolution",
                "5-minute ahead regional forecast and solution outputs by run time and target interval.",
                "one row per target interval, run time, and region",
                Some("INTERVAL_DATETIME"),
                &["RUN_DATETIME", "REGIONID"],
                &["RRP", "TOTALDEMAND", "DEMANDFORECAST"],
                &["semantic.dispatch_price on INTERVAL_DATETIME = SETTLEMENTDATE and REGIONID"],
                &[
                    "Use RUN_DATETIME to choose the forecast snapshot before comparing to actual outcomes.",
                ],
                &["forecast", "p5", "price", "demand"],
            ),
            semantic_model(
                "aemo.nemweb",
                "semantic.bid_dayoffer",
                "Daily bid price bands and rebid explanations by DUID, participant, and bid type.",
                "one row per settlement day per DUID per participant per bid type",
                Some("SETTLEMENTDATE"),
                &["DUID", "PARTICIPANTID", "BIDTYPE"],
                &[
                    "VERSIONNO",
                    "PRICEBAND1",
                    "PRICEBAND2",
                    "PRICEBAND3",
                    "PRICEBAND4",
                    "PRICEBAND5",
                    "PRICEBAND6",
                    "PRICEBAND7",
                    "PRICEBAND8",
                    "PRICEBAND9",
                    "PRICEBAND10",
                ],
                &[
                    "semantic.bid_peroffer on SETTLEMENTDATE, DUID, BIDTYPE",
                    "semantic.unit_dimension on DUID",
                ],
                &["Day offers describe offered prices, not dispatched quantities."],
                &["bids", "rebid", "offers", "fuel"],
            ),
            semantic_model(
                "aemo.nemweb",
                "semantic.bid_peroffer",
                "Interval bid availabilities by DUID and bid type.",
                "one row per interval per DUID per bid type",
                Some("INTERVAL_DATETIME"),
                &["DUID", "BIDTYPE"],
                &[
                    "MAXAVAIL",
                    "BANDAVAIL1",
                    "BANDAVAIL2",
                    "BANDAVAIL3",
                    "BANDAVAIL4",
                    "BANDAVAIL5",
                    "BANDAVAIL6",
                    "BANDAVAIL7",
                    "BANDAVAIL8",
                    "BANDAVAIL9",
                    "BANDAVAIL10",
                ],
                &["semantic.bid_dayoffer on SETTLEMENTDATE, DUID, BIDTYPE"],
                &["Band availability is an offer surface, not actual dispatch."],
                &["bids", "availability", "offers"],
            ),
            semantic_model(
                "aemo.nemweb",
                "semantic.marginal_loss_factors",
                "Marginal loss factor reference surface by DUID and connection point.",
                "one row per effective date per DUID or connection point",
                Some("EFFECTIVEDATE"),
                &["DUID", "REGIONID", "CONNECTIONPOINTID"],
                &["TRANSMISSIONLOSSFACTOR", "SECONDARY_TLF"],
                &["semantic.unit_dimension on DUID"],
                &["Reference factor table rather than interval fact data."],
                &["mlf", "loss factor", "reference"],
            ),
        ]
    }
}

fn semantic_model(
    source_id: &str,
    object_name: &str,
    description: &str,
    grain: &str,
    time_column: Option<&str>,
    dimensions: &[&str],
    measures: &[&str],
    join_keys: &[&str],
    caveats: &[&str],
    question_tags: &[&str],
) -> SemanticModel {
    SemanticModel {
        source_id: source_id.to_string(),
        object_name: object_name.to_string(),
        object_kind: "view".to_string(),
        description: description.to_string(),
        grain: grain.to_string(),
        time_column: time_column.map(str::to_string),
        dimensions: dimensions
            .iter()
            .map(|value| (*value).to_string())
            .collect(),
        measures: measures.iter().map(|value| (*value).to_string()).collect(),
        join_keys: join_keys.iter().map(|value| (*value).to_string()).collect(),
        caveats: caveats.iter().map(|value| (*value).to_string()).collect(),
        question_tags: question_tags
            .iter()
            .map(|value| (*value).to_string())
            .collect(),
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

    fn semantic_jobs(&self) -> Vec<SemanticJob> {
        let semantic_registry_sql = semantic_model_registry_sql(&self.semantic_models());
        vec![
            SemanticJob::ConsolidateObservedSchemaViews {
                target_database: "semantic".to_string(),
                include_latest_alias: true,
                naming_strategy: SemanticNamingStrategy::Default,
            },
            SemanticJob::SqlView {
                target_database: "semantic".to_string(),
                view_name: "nemweb_table_locator".to_string(),
                required_objects: vec!["raw_aemo_nemweb.observed_schemas".to_string()],
                sql: "SELECT logical_section, logical_table, report_version, physical_table, 'raw_aemo_nemweb' AS database_name, column_count, schema_hash, first_seen_at, last_seen_at FROM raw_aemo_nemweb.observed_schemas WHERE physical_table != '' GROUP BY logical_section, logical_table, report_version, physical_table, column_count, schema_hash, first_seen_at, last_seen_at".to_string(),
            },
            SemanticJob::SqlView {
                target_database: "semantic".to_string(),
                view_name: "nemweb_schema_registry".to_string(),
                required_objects: vec!["raw_aemo_nemweb.observed_schemas".to_string()],
                sql: "SELECT DISTINCT logical_section, logical_table, report_version, physical_table, column_count, schema_hash, min(first_seen_at) AS first_seen, max(last_seen_at) AS last_seen FROM raw_aemo_nemweb.observed_schemas GROUP BY logical_section, logical_table, report_version, physical_table, column_count, schema_hash".to_string(),
            },
            SemanticJob::SqlView {
                target_database: "semantic".to_string(),
                view_name: "nemweb_model_registry".to_string(),
                required_objects: Vec::new(),
                sql: semantic_registry_sql,
            },
            SemanticJob::SqlView {
                target_database: "semantic".to_string(),
                view_name: "table_locator".to_string(),
                required_objects: vec![
                    "semantic.nemweb_table_locator".to_string(),
                    "semantic.mmsdm_table_locator".to_string(),
                ],
                sql: "SELECT * FROM semantic.nemweb_table_locator UNION ALL SELECT * FROM semantic.mmsdm_table_locator".to_string(),
            },
            SemanticJob::SqlView {
                target_database: "semantic".to_string(),
                view_name: "schema_registry".to_string(),
                required_objects: vec![
                    "semantic.nemweb_schema_registry".to_string(),
                    "semantic.mmsdm_schema_registry".to_string(),
                ],
                sql: "SELECT * FROM semantic.nemweb_schema_registry UNION ALL SELECT * FROM semantic.mmsdm_schema_registry".to_string(),
            },
            SemanticJob::SqlView {
                target_database: "semantic".to_string(),
                view_name: "semantic_model_registry".to_string(),
                required_objects: vec![
                    "semantic.nemweb_model_registry".to_string(),
                    "semantic.mmsdm_model_registry".to_string(),
                ],
                sql: "SELECT * FROM semantic.nemweb_model_registry UNION ALL SELECT * FROM semantic.mmsdm_model_registry".to_string(),
            },
            SemanticJob::SqlView {
                target_database: "semantic".to_string(),
                view_name: "actual_gen_duid".to_string(),
                required_objects: vec!["semantic.meter_data_gen_duid".to_string()],
                sql: "SELECT * FROM semantic.meter_data_gen_duid".to_string(),
            },
            SemanticJob::SqlView {
                target_database: "semantic".to_string(),
                view_name: "bid_dayoffer".to_string(),
                required_objects: vec!["semantic.bid_biddayoffer_d".to_string()],
                sql: "SELECT * FROM semantic.bid_biddayoffer_d".to_string(),
            },
            SemanticJob::SqlView {
                target_database: "semantic".to_string(),
                view_name: "bid_peroffer".to_string(),
                required_objects: vec!["semantic.bid_bidperoffer_d".to_string()],
                sql: "SELECT * FROM semantic.bid_bidperoffer_d".to_string(),
            },
            SemanticJob::SqlView {
                target_database: "semantic".to_string(),
                view_name: "daily_region_dispatch".to_string(),
                required_objects: vec!["semantic.dregion".to_string()],
                sql: "SELECT * FROM semantic.dregion".to_string(),
            },
            SemanticJob::SqlView {
                target_database: "semantic".to_string(),
                view_name: "daily_unit_dispatch".to_string(),
                required_objects: vec!["semantic.dunit".to_string()],
                sql: "SELECT * FROM semantic.dunit".to_string(),
            },
            SemanticJob::SqlView {
                target_database: "semantic".to_string(),
                view_name: "dispatch_regionfcas".to_string(),
                required_objects: vec!["semantic.dispatch_regionfcasrequirement".to_string()],
                sql: "SELECT * FROM semantic.dispatch_regionfcasrequirement".to_string(),
            },
            SemanticJob::SqlView {
                target_database: "semantic".to_string(),
                view_name: "intermittent_gen_scada".to_string(),
                required_objects: vec!["semantic.demand_intermittent_gen_scada".to_string()],
                sql: "SELECT * FROM semantic.demand_intermittent_gen_scada".to_string(),
            },
            SemanticJob::SqlView {
                target_database: "semantic".to_string(),
                view_name: "marginal_loss_factors".to_string(),
                required_objects: vec!["semantic.daily_mlf".to_string()],
                sql: "SELECT * FROM semantic.daily_mlf".to_string(),
            },
            SemanticJob::SqlView {
                target_database: "semantic".to_string(),
                view_name: "mtpasa_duid_availability".to_string(),
                required_objects: vec!["semantic.mtpasa_duidavailability".to_string()],
                sql: "SELECT * FROM semantic.mtpasa_duidavailability".to_string(),
            },
            SemanticJob::SqlView {
                target_database: "semantic".to_string(),
                view_name: "next_day_energy_bids".to_string(),
                required_objects: vec!["semantic.bids_bidofferperiod_sparse".to_string()],
                sql: "SELECT * FROM semantic.bids_bidofferperiod_sparse".to_string(),
            },
        ]
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
        cursor: &'a ingest_core::DiscoveryCursorHint,
        ctx: &'a RunContext,
    ) -> BoxedFuture<'a, Result<Vec<DiscoveredArtifact>>> {
        Box::pin(async move {
            self.discover_collection(client, collection_id, limit, cursor, ctx)
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
        Ok(RuntimePluginParseResult::StructuredRaw { artifact })
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

    fn stream_structured_parse_events_runtime(
        &self,
        artifact: &LocalArtifact,
        _collection_id: &str,
        _ctx: &RunContext,
        sink: &mut dyn StructuredRawEventSink,
    ) -> Result<()> {
        parse::stream_local_archive_events(artifact, sink)
    }
}
