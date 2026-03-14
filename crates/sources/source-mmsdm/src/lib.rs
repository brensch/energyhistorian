use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use ingest_core::{
    ArtifactKind, ArtifactMetadata, BoxedFuture, CollectionCompletion, CompletionUnit,
    DiscoveredArtifact, DiscoveryCursorHint, DiscoveryRequest, LocalArtifact, ParseResult,
    PluginCapabilities, PromotionSpec, RawTableRowSink, RunContext, RuntimePluginParseResult,
    RuntimeSourcePlugin, SemanticJob, SemanticModel, SemanticNamingStrategy, SourceCollection,
    SourceDescriptor, SourceMetadataDocument, SourcePlugin, StructuredRawEventSink, TaskBlueprint,
    TaskKind, semantic_model_registry_sql,
};
use futures_util::future::join_all;
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
        limit: usize,
        cursor: &DiscoveryCursorHint,
        ctx: &RunContext,
    ) -> Result<Vec<DiscoveredArtifact>> {
        if collection_id != COLLECTION_ID {
            bail!("unknown MMSDM collection '{collection_id}'");
        }
        discover_public_reference_data(client, limit, cursor, ctx).await
    }

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

    fn semantic_models(&self) -> Vec<SemanticModel> {
        vec![
            semantic_model(
                "aemo.mmsdm",
                "semantic.unit_dimension",
                "Current reconciled DUID dimension derived from MMSDM registration history.",
                "one row per DUID in current effective state",
                Some("START_DATE"),
                &[
                    "DUID",
                    "REGIONID",
                    "PARTICIPANTID",
                    "EFFECTIVE_PARTICIPANTID",
                    "STATIONID",
                    "STATIONNAME",
                    "FUEL_TYPE",
                    "DISPATCHTYPE",
                    "SCHEDULE_TYPE",
                    "IS_STORAGE",
                    "IS_BIDIRECTIONAL",
                ],
                &[
                    "REGISTEREDCAPACITY_MW",
                    "MAXCAPACITY_MW",
                    "MAXSTORAGECAPACITY_MWH",
                    "CO2E_EMISSIONS_FACTOR",
                    "TRANSMISSIONLOSSFACTOR",
                    "DISTRIBUTIONLOSSFACTOR",
                    "MAX_RAMP_RATE_UP",
                    "MAX_RAMP_RATE_DOWN",
                ],
                &[
                    "semantic.daily_unit_dispatch on DUID",
                    "semantic.actual_gen_duid on DUID",
                    "semantic.bid_dayoffer on DUID",
                ],
                &["Current-state dimension built from historised MMSDM registration sources."],
                &[
                    "duid",
                    "participant",
                    "station",
                    "fuel",
                    "capacity",
                    "battery",
                ],
            ),
            semantic_model(
                "aemo.mmsdm",
                "semantic.participant_registration_dudetail",
                "Historised MMSDM DUID registration detail records.",
                "one row per effective-dated DUID detail record",
                Some("EFFECTIVEDATE"),
                &["DUID", "DISPATCHTYPE"],
                &["MAXSTORAGECAPACITY"],
                &["semantic.participant_registration_dudetailsummary on DUID"],
                &[
                    "Historised raw semantic registration surface, useful for registration timelines.",
                ],
                &["registration", "history", "battery", "duid"],
            ),
            semantic_model(
                "aemo.mmsdm",
                "semantic.participant_registration_dudetailsummary",
                "Historised MMSDM DUID summary registration records.",
                "one row per effective-dated DUID summary record",
                Some("START_DATE"),
                &[
                    "DUID",
                    "REGIONID",
                    "PARTICIPANTID",
                    "STATIONID",
                    "DISPATCHTYPE",
                    "SCHEDULE_TYPE",
                ],
                &[
                    "TRANSMISSIONLOSSFACTOR",
                    "DISTRIBUTIONLOSSFACTOR",
                    "MAX_RAMP_RATE_UP",
                    "MAX_RAMP_RATE_DOWN",
                    "MINIMUM_ENERGY_PRICE",
                    "MAXIMUM_ENERGY_PRICE",
                ],
                &["semantic.unit_dimension on DUID"],
                &["Historised MMSDM registration summary surface."],
                &["registration", "history", "duid", "participant"],
            ),
            semantic_model(
                "aemo.mmsdm",
                "semantic.participant_registration_station",
                "Historised MMSDM station metadata including station name and state.",
                "one row per station record change",
                Some("LASTCHANGED"),
                &["STATIONID", "STATIONNAME", "STATE"],
                &[],
                &["semantic.unit_dimension on STATIONID"],
                &["Station metadata is historised and may include multiple revisions per station."],
                &["station", "metadata", "registration"],
            ),
            semantic_model(
                "aemo.mmsdm",
                "semantic.participant_registration_stationowner",
                "Historised MMSDM station ownership records.",
                "one row per effective-dated station owner record",
                Some("EFFECTIVEDATE"),
                &["STATIONID", "PARTICIPANTID"],
                &[],
                &["semantic.unit_dimension on STATIONID"],
                &[
                    "Ownership can change over time; unit_dimension carries only the current effective owner.",
                ],
                &["ownership", "station", "participant"],
            ),
            semantic_model(
                "aemo.mmsdm",
                "semantic.participant_registration_genunits",
                "Historised MMSDM generating unit records with energy source, capacity, and emissions factors.",
                "one row per GENSET record change",
                Some("LASTCHANGED"),
                &["GENSETID", "GENSETTYPE", "CO2E_ENERGY_SOURCE"],
                &["REGISTEREDCAPACITY", "MAXCAPACITY", "CO2E_EMISSIONS_FACTOR"],
                &["semantic.unit_dimension on GENSETID"],
                &[
                    "Fuel and emissions metadata originates here before current-state reconciliation into unit_dimension.",
                ],
                &["fuel", "capacity", "emissions", "registration"],
            ),
        ]
    }
}

#[allow(clippy::too_many_arguments)]
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

    fn semantic_jobs(&self) -> Vec<SemanticJob> {
        let semantic_registry_sql = semantic_model_registry_sql(&self.semantic_models());
        vec![
            SemanticJob::ConsolidateObservedSchemaViews {
                target_database: "semantic".to_string(),
                include_latest_alias: true,
                naming_strategy: SemanticNamingStrategy::StripYearTokens,
                dedupe_rules: Vec::new(),
            },
            SemanticJob::SqlView {
                target_database: "semantic".to_string(),
                view_name: "mmsdm_table_locator".to_string(),
                required_objects: vec!["raw_aemo_mmsdm.observed_schemas".to_string()],
                sql: "SELECT logical_section, logical_table, report_version, physical_table, 'raw_aemo_mmsdm' AS database_name, column_count, schema_hash, first_seen_at, last_seen_at FROM raw_aemo_mmsdm.observed_schemas WHERE physical_table != '' GROUP BY logical_section, logical_table, report_version, physical_table, column_count, schema_hash, first_seen_at, last_seen_at".to_string(),
            },
            SemanticJob::SqlView {
                target_database: "semantic".to_string(),
                view_name: "mmsdm_schema_registry".to_string(),
                required_objects: vec!["raw_aemo_mmsdm.observed_schemas".to_string()],
                sql: "SELECT DISTINCT logical_section, logical_table, report_version, physical_table, column_count, schema_hash, min(first_seen_at) AS first_seen, max(last_seen_at) AS last_seen FROM raw_aemo_mmsdm.observed_schemas GROUP BY logical_section, logical_table, report_version, physical_table, column_count, schema_hash".to_string(),
            },
            SemanticJob::SqlView {
                target_database: "semantic".to_string(),
                view_name: "mmsdm_model_registry".to_string(),
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
                view_name: "unit_dimension".to_string(),
                required_objects: vec![
                    "semantic.participant_registration_dudetailsummary".to_string(),
                    "semantic.participant_registration_dudetail".to_string(),
                    "semantic.participant_registration_dualloc".to_string(),
                    "semantic.participant_registration_station".to_string(),
                    "semantic.participant_registration_stationowner".to_string(),
                    "semantic.participant_registration_genunits".to_string(),
                ],
                sql: concat!(
                    "WITH toDateTime64('1900-01-01 00:00:00', 3) AS epoch, ",
                    "current_summary AS (",
                    "SELECT DUID, ",
                    "tupleElement(summary_row, 1) AS REGIONID, ",
                    "tupleElement(summary_row, 2) AS PARTICIPANTID, ",
                    "tupleElement(summary_row, 3) AS STATIONID, ",
                    "tupleElement(summary_row, 4) AS DISPATCHTYPE, ",
                    "tupleElement(summary_row, 5) AS SCHEDULE_TYPE, ",
                    "tupleElement(summary_row, 6) AS CONNECTIONPOINTID, ",
                    "tupleElement(summary_row, 7) AS TRANSMISSIONLOSSFACTOR, ",
                    "tupleElement(summary_row, 8) AS DISTRIBUTIONLOSSFACTOR, ",
                    "tupleElement(summary_row, 9) AS MAX_RAMP_RATE_UP, ",
                    "tupleElement(summary_row, 10) AS MAX_RAMP_RATE_DOWN, ",
                    "tupleElement(summary_row, 11) AS MINIMUM_ENERGY_PRICE, ",
                    "tupleElement(summary_row, 12) AS MAXIMUM_ENERGY_PRICE, ",
                    "tupleElement(summary_row, 13) AS START_DATE, ",
                    "tupleElement(summary_row, 14) AS END_DATE ",
                    "FROM (",
                    "SELECT DUID, ",
                    "argMax(tuple(REGIONID, PARTICIPANTID, STATIONID, DISPATCHTYPE, SCHEDULE_TYPE, CONNECTIONPOINTID, TRANSMISSIONLOSSFACTOR, DISTRIBUTIONLOSSFACTOR, MAX_RAMP_RATE_UP, MAX_RAMP_RATE_DOWN, MINIMUM_ENERGY_PRICE, MAXIMUM_ENERGY_PRICE, START_DATE, END_DATE), tuple(coalesce(START_DATE, epoch), processed_at)) AS summary_row ",
                    "FROM semantic.participant_registration_dudetailsummary GROUP BY DUID)), ",
                    "current_detail AS (",
                    "SELECT DUID, ",
                    "tupleElement(detail_row, 1) AS MAXSTORAGECAPACITY, ",
                    "tupleElement(detail_row, 2) AS DETAIL_DISPATCHTYPE ",
                    "FROM (",
                    "SELECT DUID, ",
                    "argMax(tuple(MAXSTORAGECAPACITY, DISPATCHTYPE), tuple(coalesce(EFFECTIVEDATE, epoch), processed_at)) AS detail_row ",
                    "FROM semantic.participant_registration_dudetail GROUP BY DUID)), ",
                    "current_alloc AS (",
                    "SELECT DUID, tupleElement(alloc_row, 1) AS GENSETID ",
                    "FROM (",
                    "SELECT DUID, argMax(tuple(GENSETID), tuple(coalesce(EFFECTIVEDATE, epoch), processed_at)) AS alloc_row ",
                    "FROM semantic.participant_registration_dualloc GROUP BY DUID)), ",
                    "current_station AS (",
                    "SELECT STATIONID, ",
                    "tupleElement(station_row, 1) AS STATIONNAME, ",
                    "tupleElement(station_row, 2) AS STATE ",
                    "FROM (",
                    "SELECT STATIONID, ",
                    "argMax(tuple(STATIONNAME, STATE), tuple(coalesce(LASTCHANGED, epoch), processed_at)) AS station_row ",
                    "FROM semantic.participant_registration_station GROUP BY STATIONID)), ",
                    "current_owner AS (",
                    "SELECT STATIONID, tupleElement(owner_row, 1) AS STATION_OWNER_PARTICIPANTID ",
                    "FROM (",
                    "SELECT STATIONID, ",
                    "argMax(tuple(PARTICIPANTID), tuple(coalesce(EFFECTIVEDATE, epoch), processed_at)) AS owner_row ",
                    "FROM semantic.participant_registration_stationowner GROUP BY STATIONID)), ",
                    "current_gen AS (",
                    "SELECT GENSETID, ",
                    "tupleElement(gen_row, 1) AS GENSETTYPE, ",
                    "tupleElement(gen_row, 2) AS CO2E_ENERGY_SOURCE, ",
                    "tupleElement(gen_row, 3) AS REGISTEREDCAPACITY, ",
                    "tupleElement(gen_row, 4) AS MAXCAPACITY, ",
                    "tupleElement(gen_row, 5) AS CO2E_EMISSIONS_FACTOR ",
                    "FROM (",
                    "SELECT GENSETID, ",
                    "argMax(tuple(GENSETTYPE, CO2E_ENERGY_SOURCE, REGISTEREDCAPACITY, MAXCAPACITY, CO2E_EMISSIONS_FACTOR), tuple(coalesce(LASTCHANGED, epoch), processed_at)) AS gen_row ",
                    "FROM semantic.participant_registration_genunits GROUP BY GENSETID)) ",
                    "SELECT summary.DUID AS DUID, summary.REGIONID, summary.PARTICIPANTID, ",
                    "coalesce(owner.STATION_OWNER_PARTICIPANTID, summary.PARTICIPANTID) AS EFFECTIVE_PARTICIPANTID, ",
                    "summary.STATIONID AS STATIONID, station.STATIONNAME, station.STATE AS STATIONSTATE, alloc.GENSETID AS GENSETID, ",
                    "summary.DISPATCHTYPE, detail.DETAIL_DISPATCHTYPE, summary.SCHEDULE_TYPE, summary.CONNECTIONPOINTID, ",
                    "gen.GENSETTYPE, gen.CO2E_ENERGY_SOURCE AS ENERGY_SOURCE, ",
                    "coalesce(nullIf(gen.CO2E_ENERGY_SOURCE, ''), nullIf(gen.GENSETTYPE, ''), 'UNKNOWN') AS FUEL_TYPE, ",
                    "gen.REGISTEREDCAPACITY AS REGISTEREDCAPACITY_MW, gen.MAXCAPACITY AS MAXCAPACITY_MW, ",
                    "detail.MAXSTORAGECAPACITY AS MAXSTORAGECAPACITY_MWH, gen.CO2E_EMISSIONS_FACTOR, ",
                    "summary.TRANSMISSIONLOSSFACTOR, summary.DISTRIBUTIONLOSSFACTOR, ",
                    "summary.MAX_RAMP_RATE_UP, summary.MAX_RAMP_RATE_DOWN, ",
                    "summary.MINIMUM_ENERGY_PRICE, summary.MAXIMUM_ENERGY_PRICE, ",
                    "summary.START_DATE, summary.END_DATE, ",
                    "if(coalesce(detail.MAXSTORAGECAPACITY, 0) > 0 OR summary.DISPATCHTYPE = 'BIDIRECTIONAL', 1, 0) AS IS_STORAGE, ",
                    "if(summary.DISPATCHTYPE = 'BIDIRECTIONAL', 1, 0) AS IS_BIDIRECTIONAL ",
                    "FROM current_summary AS summary ",
                    "LEFT JOIN current_detail AS detail ON summary.DUID = detail.DUID ",
                    "LEFT JOIN current_alloc AS alloc ON summary.DUID = alloc.DUID ",
                    "LEFT JOIN current_station AS station ON summary.STATIONID = station.STATIONID ",
                    "LEFT JOIN current_owner AS owner ON summary.STATIONID = owner.STATIONID ",
                    "LEFT JOIN current_gen AS gen ON alloc.GENSETID = gen.GENSETID"
                )
                .to_string(),
            },
        ]
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
        limit: usize,
        cursor: &'a DiscoveryCursorHint,
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

/// Discovers MMSDM artifacts using a two-phase schedule:
///
/// Phase 1 (skeleton): Fetch root + year pages to build a sorted list of all
/// month slots. This is ~19 HTTP requests and takes a few seconds.
///
/// Phase 2 (batch): Fetch DATA/ directories for up to `limit` months that
/// haven't been discovered yet (based on cursor). Return those artifacts.
/// The orchestrator will call back quickly when results hit the limit.
async fn discover_public_reference_data(
    client: &reqwest::Client,
    limit: usize,
    cursor: &DiscoveryCursorHint,
    ctx: &RunContext,
) -> Result<Vec<DiscoveredArtifact>> {
    let href_re = Regex::new(r#"HREF="([^"]+)""#)?;
    let zip_re = Regex::new(r#"PUBLIC_ARCHIVE#([A-Z0-9_]+)#FILE(\d+)#([0-9]{6,12})\.zip"#)?;

    // Phase 1: skeleton crawl — build the full month schedule.
    let month_slots = build_month_schedule(client).await?;
    tracing::info!(
        total_months = month_slots.len(),
        "mmsdm: skeleton crawl complete"
    );

    // Filter to months not yet covered by cursor.
    let earliest_month = cursor
        .latest_release_name
        .as_deref()
        .and_then(parse_release_month);

    let pending: Vec<_> = month_slots
        .into_iter()
        .filter(|slot| {
            if let Some((cursor_year, cursor_month)) = earliest_month {
                (slot.year, slot.month) >= (cursor_year, cursor_month)
            } else {
                true
            }
        })
        .collect();

    tracing::info!(
        pending = pending.len(),
        cursor = ?earliest_month,
        "mmsdm: months to discover"
    );

    // Phase 2: fetch DATA/ for a batch of months.
    // We limit by number of months crawled, not artifacts, to keep HTTP cost predictable.
    let batch_month_limit = limit.max(1);
    let mut artifacts = Vec::new();
    let mut months_crawled = 0usize;

    for slot in &pending {
        if months_crawled >= batch_month_limit {
            break;
        }
        let month_artifacts =
            discover_month_artifacts(client, &slot.data_url, &slot.month_key, ctx, &href_re, &zip_re)
                .await?;
        artifacts.extend(month_artifacts);
        months_crawled += 1;
    }

    artifacts.sort_by(|left, right| left.metadata.artifact_id.cmp(&right.metadata.artifact_id));
    tracing::info!(
        months_crawled,
        total_pending = pending.len(),
        artifacts = artifacts.len(),
        "mmsdm: discovery batch complete"
    );

    Ok(artifacts)
}

/// A discovered month slot with its precomputed DATA directory URL.
struct MonthSlot {
    year: i32,
    month: u32,
    month_key: String,  // "2024-01"
    data_url: String,   // full URL to the DATA/ directory
}

/// Fetch the root and all year pages to build a sorted list of month slots.
/// This is ~19 HTTP requests (1 root + 18 years) and completes in a few seconds.
async fn build_month_schedule(client: &reqwest::Client) -> Result<Vec<MonthSlot>> {
    let year_re = Regex::new(r#"HREF="[^"]*?(\d{4})/""#)?;
    let month_re = Regex::new(r#"(MMSDM_(\d{4})_(\d{2}))/"#)?;

    // Fetch root to get year list.
    let root_html = fetch_text(client, MMSDM_ARCHIVE_ROOT).await?;
    let mut years: Vec<String> = year_re
        .captures_iter(&root_html)
        .filter_map(|captures| captures.get(1).map(|m| m.as_str().to_string()))
        .collect();
    years.sort();
    years.dedup();
    tracing::info!(count = years.len(), "mmsdm: discovered year directories");

    // Fetch all year pages concurrently to get month lists.
    let year_futures: Vec<_> = years
        .iter()
        .map(|year| {
            let url = format!("{MMSDM_ARCHIVE_ROOT}{year}/");
            async move {
                let html = fetch_text(client, &url).await;
                (year.clone(), html)
            }
        })
        .collect();
    let year_results = join_all(year_futures).await;

    let mut slots = Vec::new();
    for (year, html_result) in year_results {
        let year_html = match html_result {
            Ok(html) => html,
            Err(err) => {
                tracing::warn!(%year, %err, "mmsdm: failed to fetch year page, skipping");
                continue;
            }
        };
        for captures in month_re.captures_iter(&year_html) {
            let Some(month_dir) = captures.get(1).map(|m| m.as_str()) else {
                continue;
            };
            let Some(y) = captures.get(2).and_then(|m| m.as_str().parse::<i32>().ok()) else {
                continue;
            };
            let Some(m) = captures.get(3).and_then(|m| m.as_str().parse::<u32>().ok()) else {
                continue;
            };
            slots.push(MonthSlot {
                year: y,
                month: m,
                month_key: format!("{y}-{m:02}"),
                data_url: format!(
                    "{MMSDM_ARCHIVE_ROOT}{year}/{month_dir}/MMSDM_Historical_Data_SQLLoader/DATA/"
                ),
            });
        }
    }

    slots.sort_by_key(|s| (s.year, s.month));
    slots.dedup_by_key(|s| (s.year, s.month));
    Ok(slots)
}

async fn discover_month_artifacts(
    client: &reqwest::Client,
    data_url: &str,
    month_key: &str,
    ctx: &RunContext,
    href_re: &Regex,
    zip_re: &Regex,
) -> Result<Vec<DiscoveredArtifact>> {
    let data_html = match fetch_text(client, data_url).await {
        Ok(html) => html,
        Err(err) => {
            tracing::warn!(month_key, %err, "mmsdm: failed to fetch DATA directory, skipping month");
            return Ok(Vec::new());
        }
    };
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

fn validate_zip_payload(url: &str, bytes: &[u8]) -> Result<()> {
    const ZIP_PREFIXES: [&[u8]; 3] = [b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"];
    if ZIP_PREFIXES.iter().any(|prefix| bytes.starts_with(prefix)) {
        return Ok(());
    }
    let preview = String::from_utf8_lossy(&bytes[..bytes.len().min(160)]);
    bail!("downloaded non-zip payload from {url}: {preview}");
}
