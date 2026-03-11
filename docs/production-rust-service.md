# Production Rust Ingestion Service

## Why the current prototype is not enough

The current prototype proved four things:

- Rust can discover and fetch live NEMweb reports.
- NEMweb CSV payloads need custom parsing because they use `C` / `I` / `D` records rather than plain tabular CSV.
- Dagster can orchestrate the Rust process.
- ClickHouse is a good local analytical landing target for the first slice.

It is not yet a production ingestion platform.

The main gaps are:

- it hardcodes a small subset of tables from a source family
- it does not persist source schemas as first-class metadata
- it does not model one archive as many outputs cleanly
- it does not treat schema evolution as a governed process
- it uses ad hoc ClickHouse HTTP rather than the Rust client library
- it does not separate raw replayable capture from typed promoted datasets strongly enough

## Design goals

The production Rust service should be:

- source-agnostic
- schema-aware
- replayable
- versioned
- observable
- idempotent
- extensible across future AEMO and non-AEMO sources

It should treat NEMweb as the first plugin, not the whole product.

## Source landscape to support

## Public AEMO / NEM data surfaces worth targeting

### 1. NEMWeb Current Reports

This is the live/public feed for many market reports.

- recent files in `Current`
- files later moved to `Archive`
- payloads use `C`, `I`, and `D` rows

Official references:

- https://visualisations.aemo.com.au/aemo/nemweb/index.html
- https://markets-portal-help.docs.public.aemo.com.au/Content/CSVdataFormat/CSV_File_format.htm

### 2. NEMWeb Archive Reports

AEMO states older files move to archive and up to thirteen months are retained there.

Official reference:

- https://visualisations.aemo.com.au/aemo/nemweb/index.html

### 3. Data Model Archive

This is the older historical store for data conforming to the data model.

Official reference:

- https://visualisations.aemo.com.au/aemo/nemweb/index.html
- https://markets-portal-help.docs.public.aemo.com.au/Content/InformationSystems/Electricity/HistoricalData.htm

### 4. EMMS Data Model / Data Interchange ecosystem

This is broader than public NEMweb. It matters because it defines:

- file IDs
- target MMS / EMMS tables
- report versions
- release-driven schema changes
- legacy/current migration rules

Official references:

- https://tech-specs.docs.public.aemo.com.au/Content/TSP_TechnicalSpecificationPortal/ReleaseInformation.htm
- https://tech-specs.docs.public.aemo.com.au/Content/TSP_EMMSDM56_Nov2025/EMMS_-_DM5.6_-_Nov_2025_1.htm
- https://www.aemo.com.au/energy-systems/data-dashboards/electricity-and-gas-forecasting/-/media/Files/Electricity/NEM/IT-Systems-and-Change/EMMS-Data-Interchange-Guide.pdf

### 5. Historical DVD data

This is important. AEMO explicitly states monthly and annual historical data on DVD are available for subscription and are intended for historical analysis, processed separately from current data flows.

That makes the DVD line a valid future historical source plugin, not a side note.

Official reference:

- https://www.aemo.com.au/energy-systems/data-dashboards/electricity-and-gas-forecasting/-/media/Files/Electricity/NEM/IT-Systems-and-Change/EMMS-Data-Interchange-Guide.pdf

### 6. Other public AEMO electricity datasets that are good follow-on sources

- generation information
- DER data downloads
- congestion / statistical reporting streams
- operating incident reports

Official references:

- https://www.aemo.com.au/Electricity/National-Electricity-Market-NEM/Planning-and-forecasting/Generation-information
- https://www.aemo.com.au/en/energy-systems/electricity/der-register/data-der/data-downloads
- https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/system-operations/congestion-information-resource/statistical-reporting-streams
- https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/nem-events-and-reports/power-system-operating-incident-reports

## What this implies architecturally

The service cannot be designed around:

- one source family = one schema
- one archive = one output table
- one parser = one static record layout

Instead it needs to model:

- many source families
- many logical tables inside one artifact
- many schema versions for one logical table over time
- public and participant-only feeds
- replay/backfill from historical media

## Support all Data Model versions

This should be an explicit requirement.

Do not build the metadata or parser stack around only the current Electricity Data Model release.

The AEMO material shows multiple active and historical version lines and explicit release migration behavior:

- v5.2
- v5.3
- v5.5
- v5.6
- announced v5.7 changes
- older historical technical specifications and legacy report behavior

Official references:

- https://tech-specs.docs.public.aemo.com.au/Content/TSP_EMMSDM52May2023/Introduction.htm
- https://tech-specs.docs.public.aemo.com.au/Content/TSP_EMMSDM53_April2024/Participant_Impact.htm
- https://tech-specs.docs.public.aemo.com.au/Content/TSP_EMMSDM55_Apr2025/EMMS_DMv5.5_Apr25_1.htm
- https://tech-specs.docs.public.aemo.com.au/Content/TSP_EMMSDM56_Nov2025/EMMS_-_DM5.6_-_Nov_2025_1.htm
- https://tech-specs.docs.public.aemo.com.au/Content/TSP_EMMS_9Aug2026/August_2026.htm
- https://aemo.com.au/-/media/Files/Electricity/NEM/IT-Systems-and-Change/2017/EMMS-Technical-Specification---October-to-December-2017.pdf

## What "all versions" should mean

Support for all versions does not mean one flat universal schema.

It should mean:

- you can ingest metadata documents for every accessible version
- you can register table and report definitions by version
- you can parse historical files against the schema that was valid at that time
- you can preserve old and new structures without flattening them incorrectly
- you can promote historical and current variants into a conformed analytical layer deliberately

## Versioned metadata model

Add these keys to every metadata record:

- `model_family`
- `model_version`
- `release_name`
- `effective_from`
- `effective_to`
- `source_document_url`
- `document_hash`

For table definitions:

- `table_name`
- `package_id`
- `table_signature_hash`
- `supersedes_table_signature_hash`

For file/report mappings:

- `file_id`
- `report_type`
- `report_subtype`
- `report_version`
- `legacy_status`

This is how you avoid losing history when AEMO renames, replaces, or versions reports.

## Parsing strategy across versions

Use a three-layer strategy.

### Layer 1: document ingestion

Ingest every accessible metadata version as raw source artifacts:

- HTML pages
- PDF reports
- CSV exports

### Layer 2: normalized versioned metadata

Normalize each release into structured metadata tables:

- `aemo_meta.model_versions`
- `aemo_meta.tables`
- `aemo_meta.columns`
- `aemo_meta.report_mappings`
- `aemo_meta.upgrade_changes`

with `model_version` carried everywhere.

### Layer 3: canonical semantic mapping

Build your own stable semantic layer above AEMO version churn:

- `canonical_dataset`
- `canonical_metric`
- `canonical_dimension`
- `mapping_rule`

This allows:

- old `PD7DAY_*_PRE53` structures
- new `PD7DAY` structures
- legacy/current report pairs

to coexist while still being explainable to users and LLMs.

## Promotion policy across versions

Never merge different AEMO versions straight into the same typed table unless you have an explicit mapping rule.

Use one of these outcomes:

- `same promoted dataset`
  - only when the schema and grain are genuinely compatible
- `new promoted dataset version`
  - when business meaning is the same but structure changed materially
- `raw-only until mapped`
  - when a newly encountered version exists but no safe promotion rule exists yet

## Historical DVD and archive value here

The reason DVDs and archives matter is exactly this requirement.

If you want all versions to work, then current NEMweb alone is insufficient. You need:

- current reports
- archive reports
- data model archive
- historical DVD distributions where available

These give you the physical source artifacts that correspond to older schema versions.

## Recommended implementation rule

The Rust service should never ask:

- "what is the schema of TRADING.PRICE?"

It should ask:

- "what is the schema of TRADING.PRICE under model version X and report version Y?"

That is the correct production framing.

## Production service shape

## Core principle

The Rust service should have one job:

`capture source artifacts faithfully, register schemas explicitly, emit governed datasets, and load approved outputs into warehouse storage`

Dagster should orchestrate it, not be the source of truth for source semantics.

## Service components

### 1. Source Registry

Registry of every supported source plugin.

Each source plugin defines:

- discovery strategy
- fetch strategy
- artifact type
- parser family
- schema registration policy
- promoted dataset mappings

Suggested Rust trait:

```rust
pub trait SourcePlugin {
    fn source_id(&self) -> &'static str;
    fn discover(&self, ctx: &RunContext) -> Result<Vec<DiscoveredArtifact>>;
    fn fetch(&self, artifact: &DiscoveredArtifact, ctx: &RunContext) -> Result<LocalArtifact>;
    fn parse(&self, artifact: &LocalArtifact, ctx: &RunContext) -> Result<ParseResult>;
    fn promotion_plan(&self) -> &'static [PromotionSpec];
}
```

### 2. Artifact Store

Immutable raw store for original files.

Every fetched artifact should record:

- source ID
- source URL or acquisition origin
- fetch timestamp
- publication timestamp if known
- content hash
- byte size
- media type
- parser version
- source release / data model version if known

Do not skip this for "small" files. This is the replay backbone.

### 3. Schema Registry

This is mandatory.

For NEMweb-like payloads, every `I` row becomes a schema observation.

Canonical schema identity:

- `source_family`
- `section`
- `table`
- `report_version`
- `header_hash`

Store:

- ordered column list
- first seen / last seen
- source artifacts observed in
- compatible predecessor if any
- status: proposed / approved / deprecated / rejected

This is the control point for schema drift.

### 4. Raw Record Store

Persist every `D` row with a reference to:

- the artifact it came from
- the schema observation that defined it
- the logical table key

Suggested raw row envelope:

```json
{
  "artifact_id": "...",
  "schema_id": "...",
  "source_family": "TRADINGIS",
  "section": "TRADING",
  "table": "PRICE",
  "report_version": "3",
  "payload": { "...": "..." },
  "ingested_at": "..."
}
```

This gives you safe replay even if promotion code changes later.

### 5. Promotion Engine

Promotion moves approved raw logical tables into typed warehouse datasets.

This is where:

- column typing
- derived timestamps
- key normalization
- conformed naming
- warehouse loading

should happen.

Raw capture should stay permissive. Promotion should be strict.

### 6. Warehouse Loader

Use the Rust `clickhouse` client crate, not hand-rolled HTTP, for ongoing production usage.

Recommended use:

- typed row structs for promoted datasets
- async insert batches
- explicit load errors on incompatible schema
- ClickHouse DDL handled through versioned migration files, not inline strings as the long-term pattern

Reference:

- https://docs.rs/clickhouse

### 7. State Store

Persist ingestion state outside process memory:

- last discovered cursors
- artifact processing status
- retry state
- schema change events
- promotion checkpoints

This can live in PostgreSQL for operational metadata even if ClickHouse is the analytical warehouse.

That split is sane:

- `PostgreSQL`: operational control plane
- `ClickHouse`: analytical data plane

## Data model inside the ingestion service

## Operational metadata tables

These should exist regardless of the warehouse choice:

- `op.source_plugins`
- `op.artifacts`
- `op.artifact_fetch_attempts`
- `op.logical_tables`
- `op.schema_versions`
- `op.schema_columns`
- `op.schema_change_events`
- `op.promotions`
- `op.load_batches`
- `op.data_quality_checks`

## Semantic distinction you need

Make these distinctions explicit:

- `artifact`
  - one physical ZIP / CSV / DVD extract / API response file
- `logical table`
  - one stream such as `TRADING.PRICE`
- `schema version`
  - one concrete header layout for a logical table
- `promoted dataset`
  - one approved typed warehouse table

Those are not the same thing.

## NEMweb parser model

## Treat the CSV format properly

AEMO documents the CSV payload as:

- `C` = comment / metadata
- `I` = information
- `D` = data

Do not call the separator "CID header" in code. Be precise:

- `C row`
- `I row`
- `D row`

The parser should:

1. read all `C` rows and attach artifact-level metadata
2. read each `I` row and register a schema observation
3. route following `D` rows to the matching `(section, table, version)` stream
4. emit one output batch per logical table per schema version

That means one archive can yield many outputs cleanly.

## Promotion policy for schema change

Use explicit rules.

### Compatible change

- additive nullable column
- metadata/comment-only change

Action:

- register new schema observation
- optionally allow promotion if mapping says safe

### Incompatible change

- renamed column
- removed column
- changed column meaning
- changed key semantics
- changed version with altered grain

Action:

- keep raw capture running
- block typed promotion for that dataset
- raise alert / asset check failure
- require explicit mapper update

Never silently "best effort" these changes into the same promoted table.

## Dagster mapping

Dagster should model the archive and its derived outputs separately.

## Recommended approach

Use registry-driven `multi_asset` or generated assets.

For example:

- input artifact asset:
  - `nemweb.tradingis.archive`
- derived assets:
  - `nemweb.raw.trading.price.v3`
  - `nemweb.raw.trading.interconnectorres.v2`
  - `nemweb.promoted.trading_price`
  - `nemweb.promoted.trading_interconnectorres`

This is better than one broad `nemweb_tradingis_snapshot` asset because:

- one artifact produces multiple tables
- one table can fail promotion while others succeed
- schema change alerts belong to a specific logical table

## Multi-output rule

One Dagster asset should not pretend there is one output if the source naturally produces many datasets.

For NEMweb families, the correct mapping is:

- one archive ingest event
- many raw logical-table outputs
- optionally several promoted outputs

## Recommended Rust crate choices

### Core

- `tokio`
- `reqwest`
- `serde`
- `serde_json`
- `csv`
- `zip`
- `sha2`
- `chrono`
- `thiserror` or `anyhow`

### Strongly recommended additions

- `clickhouse`
- `sqlx` or `diesel` for operational metadata store if using PostgreSQL
- `tracing`
- `tracing-subscriber`
- `schemars` if you want JSON-schema export for internal contracts

## Warehouse write strategy

## Raw layer

For production, raw capture should land in object storage and optionally a raw warehouse table.

Best pattern:

- immutable file in object storage
- raw parsed rows in ClickHouse or parquet/object storage

## Promoted layer

Only approved logical tables become typed ClickHouse datasets.

First promoted tables still make sense:

- `trading_price`
- `trading_interconnectorres`
- `dispatch_local_price`
- `dispatch_case_solution`

But they should be promotion outputs from the registry, not hand-special cases hidden in the parser.

## Recommended service layout in the repo

```text
apps/
  schedulerd/
  downloaderd/
  parserd/
libs/
  ingest-core/
    artifact/
    schema/
    parser/
    promotion/
    warehouse/
    state/
  source-nemweb/
    discover/
    fetch/
    parse/
    mappings/
  source-aemo-dvd/
    discover/
    ingest/
    extract/
    mappings/
  energyhistorian-cli/
    main.rs
```

## Commands

Suggested CLI shape:

```text
energyhistorian sources list
energyhistorian discover --source nemweb.tradingis
energyhistorian fetch --source nemweb.tradingis
energyhistorian parse --artifact <id>
energyhistorian promote --dataset nemweb.promoted.trading_price
energyhistorian backfill --source nemweb.dvd --from 2018-01 --to 2018-12
energyhistorian schemas diff --logical-table TRADING.PRICE
energyhistorian schemas approve --schema <id>
```

This is closer to an actual platform surface.

## Suggested first production backlog

### Phase 1: fix the architecture shape

- add operational metadata store
- add schema registry
- parse all `I` rows and route all logical tables
- emit raw logical-table outputs generically

### Phase 2: warehouse correctness

- switch to Rust `clickhouse` crate
- move DDL into migrations
- create promoted typed datasets through registry mappings
- add idempotent load keys

### Phase 3: orchestration correctness

- replace coarse Dagster assets with generated multi-assets
- add asset checks for schema drift, row counts, freshness, and duplicate artifacts

### Phase 4: historical backfill

- add `Archive Reports`
- add `Data Model Archive`
- add `DVD` source plugin

## NEM source candidates to prioritise after the first slice

### High value next

- unit-level dispatch / SCADA outputs
- predispatch / PASA forecast tables
- standing data / participant and DUID reference tables

These let you answer:

- which generators drove price
- forecast vs actual deviations
- generator / fuel / region joins

### High value historical

- Data Model Archive datasets
- historical DVD distributions

These matter if you want multi-year depth, not just current/archive depth.

### Good supplementary context sources

- generation information
- DER register downloads
- incident reports
- congestion / constraint reporting streams

These are not replacements for NEM dispatch data, but they improve the portal’s explanatory power.

## Bottom line

The production Rust service should be built around:

- `artifact capture`
- `schema registry`
- `raw logical-table emission`
- `strict promoted datasets`
- `official ClickHouse client`
- `Dagster as orchestration over many outputs, not one snapshot`

And yes, the historical DVD line should be planned as a first-class future source plugin.

## Sources

- NEMWeb market data landing page:
  - https://visualisations.aemo.com.au/aemo/nemweb/index.html
- Historical data help:
  - https://markets-portal-help.docs.public.aemo.com.au/Content/InformationSystems/Electricity/HistoricalData.htm
- CSV payload format:
  - https://markets-portal-help.docs.public.aemo.com.au/Content/CSVdataFormat/CSV_File_format.htm
- EMMS Data Interchange Guide:
  - https://www.aemo.com.au/energy-systems/data-dashboards/electricity-and-gas-forecasting/-/media/Files/Electricity/NEM/IT-Systems-and-Change/EMMS-Data-Interchange-Guide.pdf
- EMMS release information:
  - https://tech-specs.docs.public.aemo.com.au/Content/TSP_TechnicalSpecificationPortal/ReleaseInformation.htm
- Electricity Data Model v5.6:
  - https://tech-specs.docs.public.aemo.com.au/Content/TSP_EMMSDM56_Nov2025/EMMS_-_DM5.6_-_Nov_2025_1.htm
- Participant impact example for report/version migrations:
  - https://tech-specs.docs.public.aemo.com.au/Content/TSP_EMMSDM53_April2024/Participant_Impact.htm
- Generation information:
  - https://www.aemo.com.au/Electricity/National-Electricity-Market-NEM/Planning-and-forecasting/Generation-information
- DER data downloads:
  - https://www.aemo.com.au/en/energy-systems/electricity/der-register/data-der/data-downloads
- Statistical reporting streams:
  - https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/system-operations/congestion-information-resource/statistical-reporting-streams
- Operating incident reports:
  - https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/nem-events-and-reports/power-system-operating-incident-reports
- Rust ClickHouse client:
  - https://docs.rs/clickhouse
