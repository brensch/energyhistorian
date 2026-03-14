// ── MMSDM Semantic Layer ─────────────────────────────────────────────────
//
// The semantic layer serves two purposes:
//
// 1. **Semantic Views** (SemanticJob::SqlView):
//    ClickHouse views that transform raw ingested MMSDM data into clean,
//    user-facing analytical surfaces.  For example, the `unit_dimension`
//    view joins 6 raw registration tables to produce a single "current
//    state" lookup for every generating unit (DUID) in the NEM.
//
//    These views live in the `semantic` database in ClickHouse and are
//    what the AI API queries when users ask questions.
//
// 2. **Semantic Model Registry** (SemanticModel structs):
//    Metadata about each semantic view: what columns it has, what the
//    grain is, how it joins to other views, and what kinds of questions
//    it can answer.  This metadata is itself materialised as a ClickHouse
//    view (`mmsdm_model_registry`) so the AI can discover and reason
//    about available data surfaces.
//
//    Think of SemanticModel as "documentation that the LLM can query".
//
// The raw MMSDM tables are ingested into `raw_aemo_mmsdm.*` by the
// parser.  The ConsolidateObservedSchemaViews job auto-creates views
// over those raw tables (stripping year tokens from names so e.g.
// `participant_registration_dudetailsummary_202401` becomes accessible
// as `semantic.participant_registration_dudetailsummary`).

use ingest_core::{
    SemanticJob, SemanticModel, SemanticNamingStrategy, semantic_model_registry_sql,
};

/// Build all semantic model metadata entries for the MMSDM source.
///
/// Each entry describes one semantic view: its grain, columns, join keys,
/// and "question tags" (keywords the AI uses to decide which view to query).
pub fn semantic_models() -> Vec<SemanticModel> {
    vec![
        // ── unit_dimension ───────────────────────────────────────────
        //
        // The single most important semantic view for MMSDM.  It joins
        // all the registration tables to produce one row per DUID with
        // current-state attributes: region, fuel type, capacity, owner,
        // station name, loss factors, etc.
        //
        // Almost every query about "which generators" or "what fuel type"
        // joins through this view.
        SemanticModel {
            source_id: "aemo.mmsdm".into(),
            object_name: "semantic.unit_dimension".into(),
            object_kind: "view".into(),
            description: "Current reconciled DUID dimension derived from MMSDM \
                registration history.  One row per DUID showing its current \
                region, fuel type, capacity, station, owner, and loss factors."
                .into(),
            grain: "one row per DUID in current effective state".into(),
            time_column: Some("START_DATE".into()),
            dimensions: vec![
                "DUID".into(),
                "REGIONID".into(),
                "PARTICIPANTID".into(),
                "EFFECTIVE_PARTICIPANTID".into(),
                "STATIONID".into(),
                "STATIONNAME".into(),
                "FUEL_TYPE".into(),
                "DISPATCHTYPE".into(),
                "SCHEDULE_TYPE".into(),
                "IS_STORAGE".into(),
                "IS_BIDIRECTIONAL".into(),
            ],
            measures: vec![
                "REGISTEREDCAPACITY_MW".into(),
                "MAXCAPACITY_MW".into(),
                "MAXSTORAGECAPACITY_MWH".into(),
                "CO2E_EMISSIONS_FACTOR".into(),
                "TRANSMISSIONLOSSFACTOR".into(),
                "DISTRIBUTIONLOSSFACTOR".into(),
                "MAX_RAMP_RATE_UP".into(),
                "MAX_RAMP_RATE_DOWN".into(),
            ],
            join_keys: vec![
                "semantic.daily_unit_dispatch on DUID".into(),
                "semantic.actual_gen_duid on DUID".into(),
                "semantic.bid_dayoffer on DUID".into(),
            ],
            caveats: vec![
                "Current-state dimension built from historised MMSDM registration sources.".into(),
            ],
            question_tags: vec![
                "duid".into(),
                "participant".into(),
                "station".into(),
                "fuel".into(),
                "capacity".into(),
                "battery".into(),
            ],
        },
        // ── participant_registration_dudetail ────────────────────────
        //
        // Raw registration detail per DUID, effective-dated.  Useful
        // for seeing how a DUID's attributes changed over time (e.g.
        // when a battery was registered, when storage capacity changed).
        SemanticModel {
            source_id: "aemo.mmsdm".into(),
            object_name: "semantic.participant_registration_dudetail".into(),
            object_kind: "view".into(),
            description: "Historised MMSDM DUID registration detail records. \
                Shows how each DUID's dispatch type and storage capacity \
                changed over time."
                .into(),
            grain: "one row per effective-dated DUID detail record".into(),
            time_column: Some("EFFECTIVEDATE".into()),
            dimensions: vec!["DUID".into(), "DISPATCHTYPE".into()],
            measures: vec!["MAXSTORAGECAPACITY".into()],
            join_keys: vec![
                "semantic.participant_registration_dudetailsummary on DUID".into(),
            ],
            caveats: vec![
                "Historised raw registration surface — multiple rows per DUID.".into(),
            ],
            question_tags: vec![
                "registration".into(),
                "history".into(),
                "battery".into(),
                "duid".into(),
            ],
        },
        // ── participant_registration_dudetailsummary ─────────────────
        //
        // Summary registration record per DUID.  This is the primary
        // source for unit_dimension — it has region, participant,
        // station, loss factors, ramp rates, etc.
        SemanticModel {
            source_id: "aemo.mmsdm".into(),
            object_name: "semantic.participant_registration_dudetailsummary".into(),
            object_kind: "view".into(),
            description: "Historised MMSDM DUID summary registration records \
                with region, participant, station, loss factors, and ramp rates."
                .into(),
            grain: "one row per effective-dated DUID summary record".into(),
            time_column: Some("START_DATE".into()),
            dimensions: vec![
                "DUID".into(),
                "REGIONID".into(),
                "PARTICIPANTID".into(),
                "STATIONID".into(),
                "DISPATCHTYPE".into(),
                "SCHEDULE_TYPE".into(),
            ],
            measures: vec![
                "TRANSMISSIONLOSSFACTOR".into(),
                "DISTRIBUTIONLOSSFACTOR".into(),
                "MAX_RAMP_RATE_UP".into(),
                "MAX_RAMP_RATE_DOWN".into(),
                "MINIMUM_ENERGY_PRICE".into(),
                "MAXIMUM_ENERGY_PRICE".into(),
            ],
            join_keys: vec!["semantic.unit_dimension on DUID".into()],
            caveats: vec!["Historised MMSDM registration summary surface.".into()],
            question_tags: vec![
                "registration".into(),
                "history".into(),
                "duid".into(),
                "participant".into(),
            ],
        },
        // ── participant_registration_station ─────────────────────────
        SemanticModel {
            source_id: "aemo.mmsdm".into(),
            object_name: "semantic.participant_registration_station".into(),
            object_kind: "view".into(),
            description: "Historised MMSDM station metadata including station \
                name and state/territory."
                .into(),
            grain: "one row per station record change".into(),
            time_column: Some("LASTCHANGED".into()),
            dimensions: vec![
                "STATIONID".into(),
                "STATIONNAME".into(),
                "STATE".into(),
            ],
            measures: vec![],
            join_keys: vec!["semantic.unit_dimension on STATIONID".into()],
            caveats: vec![
                "Station metadata is historised and may include multiple revisions per station."
                    .into(),
            ],
            question_tags: vec![
                "station".into(),
                "metadata".into(),
                "registration".into(),
            ],
        },
        // ── participant_registration_stationowner ────────────────────
        SemanticModel {
            source_id: "aemo.mmsdm".into(),
            object_name: "semantic.participant_registration_stationowner".into(),
            object_kind: "view".into(),
            description: "Historised MMSDM station ownership records. \
                Shows which participant owns each station over time."
                .into(),
            grain: "one row per effective-dated station owner record".into(),
            time_column: Some("EFFECTIVEDATE".into()),
            dimensions: vec!["STATIONID".into(), "PARTICIPANTID".into()],
            measures: vec![],
            join_keys: vec!["semantic.unit_dimension on STATIONID".into()],
            caveats: vec![
                "Ownership can change over time; unit_dimension carries only the current effective owner."
                    .into(),
            ],
            question_tags: vec![
                "ownership".into(),
                "station".into(),
                "participant".into(),
            ],
        },
        // ── participant_registration_genunits ────────────────────────
        SemanticModel {
            source_id: "aemo.mmsdm".into(),
            object_name: "semantic.participant_registration_genunits".into(),
            object_kind: "view".into(),
            description: "Historised MMSDM generating unit records with \
                energy source, capacity, and emissions factors."
                .into(),
            grain: "one row per GENSET record change".into(),
            time_column: Some("LASTCHANGED".into()),
            dimensions: vec![
                "GENSETID".into(),
                "GENSETTYPE".into(),
                "CO2E_ENERGY_SOURCE".into(),
            ],
            measures: vec![
                "REGISTEREDCAPACITY".into(),
                "MAXCAPACITY".into(),
                "CO2E_EMISSIONS_FACTOR".into(),
            ],
            join_keys: vec!["semantic.unit_dimension on GENSETID".into()],
            caveats: vec![
                "Fuel and emissions metadata originates here before current-state \
                reconciliation into unit_dimension."
                    .into(),
            ],
            question_tags: vec![
                "fuel".into(),
                "capacity".into(),
                "emissions".into(),
                "registration".into(),
            ],
        },
    ]
}

/// Build all semantic jobs for the MMSDM source.
///
/// This defines the ClickHouse views and registry tables that transform
/// raw ingested data into the semantic layer.
pub fn semantic_jobs() -> Vec<SemanticJob> {
    let registry_sql = semantic_model_registry_sql(&semantic_models());
    vec![
        // ── Auto-consolidate raw tables ──────────────────────────────
        //
        // The parser creates per-schema-hash tables like:
        //   raw_aemo_mmsdm.participant_registration_dudetailsummary_abc123
        //
        // This job auto-creates UNION ALL views over all schema versions
        // so consumers don't need to know about schema evolution:
        //   semantic.participant_registration_dudetailsummary
        //
        // StripYearTokens removes year suffixes from table names so that
        // e.g. "2024_01" and "2024_02" data all appears under one view.
        SemanticJob::ConsolidateObservedSchemaViews {
            target_database: "semantic".to_string(),
            include_latest_alias: true,
            naming_strategy: SemanticNamingStrategy::StripYearTokens,
            dedupe_rules: Vec::new(),
        },
        // ── Table locator ────────────────────────────────────────────
        //
        // A view listing all physical tables we've ingested, with their
        // schema hashes and column counts.  Useful for debugging and
        // schema evolution tracking.
        SemanticJob::SqlView {
            target_database: "semantic".to_string(),
            view_name: "mmsdm_table_locator".to_string(),
            required_objects: vec!["raw_aemo_mmsdm.observed_schemas".to_string()],
            sql: "\
                SELECT logical_section, logical_table, report_version, \
                       physical_table, 'raw_aemo_mmsdm' AS database_name, \
                       column_count, schema_hash, first_seen_at, last_seen_at \
                FROM raw_aemo_mmsdm.observed_schemas \
                WHERE physical_table != '' \
                GROUP BY logical_section, logical_table, report_version, \
                         physical_table, column_count, schema_hash, \
                         first_seen_at, last_seen_at"
                .to_string(),
        },
        // ── Schema registry ──────────────────────────────────────────
        //
        // Deduplicated view of observed schemas with first/last seen dates.
        // Helps answer "when did the schema for table X change?"
        SemanticJob::SqlView {
            target_database: "semantic".to_string(),
            view_name: "mmsdm_schema_registry".to_string(),
            required_objects: vec!["raw_aemo_mmsdm.observed_schemas".to_string()],
            sql: "\
                SELECT DISTINCT logical_section, logical_table, report_version, \
                       physical_table, column_count, schema_hash, \
                       min(first_seen_at) AS first_seen, \
                       max(last_seen_at) AS last_seen \
                FROM raw_aemo_mmsdm.observed_schemas \
                GROUP BY logical_section, logical_table, report_version, \
                         physical_table, column_count, schema_hash"
                .to_string(),
        },
        // ── Model registry ───────────────────────────────────────────
        //
        // Materialises the SemanticModel metadata as a queryable view.
        // The AI API reads this to discover what semantic views exist
        // and how to use them.
        SemanticJob::SqlView {
            target_database: "semantic".to_string(),
            view_name: "mmsdm_model_registry".to_string(),
            required_objects: Vec::new(),
            sql: registry_sql,
        },
        // ── Cross-source union views ─────────────────────────────────
        //
        // These combine MMSDM and NEMweb registry/locator views into
        // unified surfaces so the AI sees one table_locator, one
        // schema_registry, and one model_registry regardless of source.
        SemanticJob::SqlView {
            target_database: "semantic".to_string(),
            view_name: "table_locator".to_string(),
            required_objects: vec![
                "semantic.nemweb_table_locator".to_string(),
                "semantic.mmsdm_table_locator".to_string(),
            ],
            sql: "SELECT * FROM semantic.nemweb_table_locator \
                  UNION ALL \
                  SELECT * FROM semantic.mmsdm_table_locator"
                .to_string(),
        },
        SemanticJob::SqlView {
            target_database: "semantic".to_string(),
            view_name: "schema_registry".to_string(),
            required_objects: vec![
                "semantic.nemweb_schema_registry".to_string(),
                "semantic.mmsdm_schema_registry".to_string(),
            ],
            sql: "SELECT * FROM semantic.nemweb_schema_registry \
                  UNION ALL \
                  SELECT * FROM semantic.mmsdm_schema_registry"
                .to_string(),
        },
        SemanticJob::SqlView {
            target_database: "semantic".to_string(),
            view_name: "semantic_model_registry".to_string(),
            required_objects: vec![
                "semantic.nemweb_model_registry".to_string(),
                "semantic.mmsdm_model_registry".to_string(),
            ],
            sql: "SELECT * FROM semantic.nemweb_model_registry \
                  UNION ALL \
                  SELECT * FROM semantic.mmsdm_model_registry"
                .to_string(),
        },
        // ── unit_dimension ───────────────────────────────────────────
        //
        // The flagship semantic view.  Joins 6 registration tables to
        // produce one row per DUID with current-state attributes.
        //
        // Strategy: for each source table, use ClickHouse's argMax()
        // to pick the most recent row per entity (by effective date,
        // falling back to processed_at for ties).  Then LEFT JOIN
        // everything together on DUID → STATIONID → GENSETID.
        //
        // The result has:
        //   - DUID identification (region, participant, station)
        //   - Physical attributes (fuel type, capacity, storage)
        //   - Grid connection (loss factors, connection point)
        //   - Operational limits (ramp rates, price limits)
        //   - Derived flags (IS_STORAGE, IS_BIDIRECTIONAL)
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
            sql: UNIT_DIMENSION_SQL.to_string(),
        },
    ]
}

/// SQL for the unit_dimension view.
///
/// This is a large query so we keep it as a separate constant for readability.
///
/// How it works:
///
/// 1. Each CTE (current_summary, current_detail, etc.) picks the "latest"
///    row per entity using argMax(tuple(...), tuple(effective_date, processed_at)).
///    This is ClickHouse's idiom for "last row by composite sort key".
///
/// 2. The epoch sentinel (1900-01-01) handles NULL effective dates so they
///    sort before any real date.
///
/// 3. The final SELECT joins everything:
///    - current_summary: DUID → region, participant, station, loss factors
///    - current_detail:  DUID → storage capacity, dispatch type detail
///    - current_alloc:   DUID → GENSETID mapping
///    - current_station: STATIONID → station name, state
///    - current_owner:   STATIONID → owner participant
///    - current_gen:     GENSETID → fuel type, capacity, emissions
///
/// 4. Derived columns:
///    - FUEL_TYPE: coalesce(CO2E_ENERGY_SOURCE, GENSETTYPE, 'UNKNOWN')
///    - IS_STORAGE: true if MAXSTORAGECAPACITY > 0 or DISPATCHTYPE = 'BIDIRECTIONAL'
///    - IS_BIDIRECTIONAL: true if DISPATCHTYPE = 'BIDIRECTIONAL'
///    - EFFECTIVE_PARTICIPANTID: owner if known, otherwise registered participant
const UNIT_DIMENSION_SQL: &str = concat!(
    "WITH toDateTime64('1900-01-01 00:00:00', 3) AS epoch, ",

    // -- current_summary: latest DUDETAILSUMMARY row per DUID --
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

    // -- current_detail: latest DUDETAIL row per DUID --
    "current_detail AS (",
    "SELECT DUID, ",
    "tupleElement(detail_row, 1) AS MAXSTORAGECAPACITY, ",
    "tupleElement(detail_row, 2) AS DETAIL_DISPATCHTYPE ",
    "FROM (",
    "SELECT DUID, ",
    "argMax(tuple(MAXSTORAGECAPACITY, DISPATCHTYPE), tuple(coalesce(EFFECTIVEDATE, epoch), processed_at)) AS detail_row ",
    "FROM semantic.participant_registration_dudetail GROUP BY DUID)), ",

    // -- current_alloc: latest DUID→GENSET mapping --
    "current_alloc AS (",
    "SELECT DUID, tupleElement(alloc_row, 1) AS GENSETID ",
    "FROM (",
    "SELECT DUID, argMax(tuple(GENSETID), tuple(coalesce(EFFECTIVEDATE, epoch), processed_at)) AS alloc_row ",
    "FROM semantic.participant_registration_dualloc GROUP BY DUID)), ",

    // -- current_station: latest station metadata --
    "current_station AS (",
    "SELECT STATIONID, ",
    "tupleElement(station_row, 1) AS STATIONNAME, ",
    "tupleElement(station_row, 2) AS STATE ",
    "FROM (",
    "SELECT STATIONID, ",
    "argMax(tuple(STATIONNAME, STATE), tuple(coalesce(LASTCHANGED, epoch), processed_at)) AS station_row ",
    "FROM semantic.participant_registration_station GROUP BY STATIONID)), ",

    // -- current_owner: latest station ownership --
    "current_owner AS (",
    "SELECT STATIONID, tupleElement(owner_row, 1) AS STATION_OWNER_PARTICIPANTID ",
    "FROM (",
    "SELECT STATIONID, ",
    "argMax(tuple(PARTICIPANTID), tuple(coalesce(EFFECTIVEDATE, epoch), processed_at)) AS owner_row ",
    "FROM semantic.participant_registration_stationowner GROUP BY STATIONID)), ",

    // -- current_gen: latest generating unit attributes --
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

    // -- Final join: assemble the dimension --
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
);
