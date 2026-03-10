# Repo Scaffold

## Intent

This scaffold is the first step from prototype to platform.

The existing `energyhistorian` crate is still the working prototype entrypoint, but the workspace now includes the production-oriented boundaries that future implementation should move into.

## Crates

### `ingest-core`

Shared contracts for:

- artifact discovery and capture
- source plugins
- schema observations
- promotion mappings
- processing state

### `source-nemweb`

Home for the production NEMweb plugin.

This should eventually own:

- source discovery
- artifact fetch
- `C` / `I` / `D` parsing
- logical-table routing
- schema observation emission

### `source-aemo-metadata`

Home for ingestion of:

- Data Model reports
- Package Summary
- table-to-report mappings
- Upgrade reports
- Data Population Dates

This is the metadata backbone for version-aware parsing.

### `source-aemo-dvd`

Home for historical media acquisition and extraction workflows.

This should eventually support deep historical backfills aligned with old data-model versions.

### `energyhistorian`

Current prototype and working dev entrypoint.

This should eventually be replaced or slimmed down into a top-level orchestration CLI after logic migrates into `ingest-core` and the source crates.

## Next implementation sequence

1. move the remaining working NEMweb fetch/parse logic from `energyhistorian` into `source-nemweb`
2. add a real schema registry store and persist all observed `I` rows
3. build the first `source-aemo-metadata` parsers for table/report relationships and Data Model reports
4. replace ad hoc ClickHouse HTTP usage with the Rust `clickhouse` crate
5. remodel Dagster assets to reflect multi-output logical tables instead of coarse snapshots
