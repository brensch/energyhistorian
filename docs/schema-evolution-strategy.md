# Schema Evolution Strategy

## Context

This repo currently captures NEMweb schema observations from `I` records and assigns each observed header a stable `header_hash`. That is a good start, but it does not yet let schema identity drive storage layout.

The older `nemscraper` project takes a harder-line approach:

- infer a physical table schema from each incoming file/table
- sort columns before hashing
- include the hash in the physical ClickHouse table name
- expose a stable query layer on top through separate views

That approach is operationally conservative because an unexpected schema change cannot silently corrupt an existing table.

## What `nemscraper` is doing

Relevant implementation points:

- [`vendor/nemscraper/pkg/process/chunk.go`](../vendor/nemscraper/pkg/process/chunk.go)
  - `generateCacheKey(...)` hashes the base table name plus sorted headers
  - `generateTableName(...)` hashes sorted `column_name:type` pairs and uses that hash in the physical table name
- [`vendor/nemscraper/pkg/process/track.go`](../vendor/nemscraper/pkg/process/track.go)
  - persists download/process status in `tracking.ledger`
- query surface relies on a stable `nemwebviews` layer rather than asking users to query hashed physical tables directly

This is not a hack. It is a legitimate versioned-physical-schema pattern.

## Comparison With Current Rust Design

Current repo strengths:

- schema identity is explicit in code:
  - `logical_table`
  - `report_version`
  - `model_version`
  - `header_hash`
- schema observations are already persisted in a registry-friendly shape
- source plugin abstraction is moving toward a generic, multi-source architecture

Current repo gaps:

- physical storage does not yet key off schema identity
- promotion and serving layers are still too hand-authored
- control-plane state is not yet authoritative
- Dagster still orchestrates coarse source snapshots rather than stable lifecycle stages

## Recommendation

Use a hybrid of the two approaches:

1. Keep versioned schema identity as first-class metadata.
2. Make physical raw tables schema-specific.
3. Expose stable logical datasets through generated views/materializations.
4. Let promotion rules map many schema variants into one canonical serving dataset when safe.

In short:

- raw physical layer: hash/version specific
- semantic layer: stable and human-facing

That is the safest design for a system expected to run for years while AEMO evolves.

## Recommended Physical Model

### 1. Artifact layer

Track every downloaded artifact independently.

Example tables:

- `control.discovered_artifacts`
- `control.fetched_artifacts`
- `control.parse_runs`

### 2. Schema registry layer

Track every observed schema version.

Example tables:

- `meta.logical_tables`
- `meta.schema_versions`
- `meta.schema_columns`
- `meta.schema_observations`

Key identity:

- `source_id`
- `collection`
- `section`
- `table`
- `report_version`
- `model_version`
- `header_hash`

### 3. Raw physical storage layer

Create one physical raw table per schema version.

Example naming:

- `raw.dispatch__price__v5__mmsdm_5_6__h_ab12cd34`
- `raw.dispatch__price__v5__unknown_model__h_ef56aa90`

Properties:

- append-only
- includes artifact metadata columns
- includes ingest metadata columns
- never mutates old schemas in place

This is where the `nemscraper` idea is correct.

### 4. Stable semantic layer

Generate stable views on top.

Example:

- `sem.dispatch__price`

This view can union compatible raw schema tables and project them into a canonical column contract.

For incompatible variants:

- leave them out until mapped, or
- expose a parallel compatibility view

### 5. Curated serving layer

Build user-facing datasets from the stable semantic layer.

Example:

- `serving.regional_prices_5min`
- `serving.constraint_shadow_prices`

These are the tables the portal and LLM query path should prefer.

## Why This Is Better Than Only Views Or Only Hashes

Only stable tables:

- unsafe under schema drift
- forces destructive `ALTER`s or silent coercion

Only hashed raw tables:

- safe, but painful for users and downstream tools

Hybrid model:

- raw layer stays safe
- semantic layer stays queryable
- promotion stays governed

## Dagster Role In This Model

Dagster should not hardcode one asset per AEMO table.

Instead it should orchestrate stable stages:

- `catalog_sync`
- `artifact_discovery`
- `artifact_fetch`
- `artifact_parse`
- `schema_registry_update`
- `raw_table_reconcile`
- `semantic_view_reconcile`
- `promotion_plan`
- `promotion_execute`
- `quality_checks`

Important point:

`raw_table_reconcile` and `semantic_view_reconcile` should be code-generated from registry state.

That means when a new schema appears:

- Rust records it
- reconciliation detects no physical raw table yet
- reconciliation creates the new raw physical table
- reconciliation updates semantic view definitions if a mapping exists

So the system can evolve automatically without regenerating Dagster definitions for every new AEMO table.

## Automatic Evolution Policy

When a new schema hash appears:

1. record it in the schema registry
2. create a new raw physical table for that exact schema
3. route matching parsed rows into that table
4. compare it to prior schema versions for the same logical table
5. attempt automatic compatibility classification:
   - identical
   - additive
   - reorder-only
   - incompatible
6. if compatible and mapped:
   - update semantic view generation
7. if incompatible:
   - quarantine from canonical semantic view
   - keep raw ingestion running
   - raise an operational alert

This is the main improvement over the current repo.

## ClickHouse Fit

ClickHouse is well suited to this pattern:

- many append-only physical tables are acceptable
- views can present a stable query surface
- materialized views can maintain curated datasets
- compression is strong enough that schema-fragmented raw storage is still practical

The correct implementation path is to use the Rust `clickhouse` client crate and make schema reconciliation explicit instead of writing ad hoc HTTP inserts.

## Decision

Adopt this as the target design:

- schema-specific raw physical tables
- stable semantic views above them
- curated serving tables above that
- Dagster orchestrating lifecycle stages
- Rust service as source of truth for discovery, parsing, schema registration, and reconciliation

That keeps the best part of `nemscraper` while fixing its main limitation: the absence of a richer version-aware metadata and control plane.
