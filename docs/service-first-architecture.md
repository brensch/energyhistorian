# Service-First Architecture

## Direction

This repo is moving away from a one-shot CLI-centric ingestion model and toward long-lived Rust services.

The target shape is:

- `schedulerd`
  - always-on scheduler and discovery service
- `downloaderd`
  - fetch workers writing immutable artifacts into S3-compatible storage
- `parserd`
  - parse workers reading artifacts and publishing raw tables
- `Postgres`
  - operational truth for discovery/fetch/parse coordination
- `ClickHouse`
  - raw, semantic, and serving data warehouse
- `MinIO` or another S3-compatible object store
  - immutable fetched artifact storage

## Why A Service

The service model is a better fit because it can:

- coordinate work in Postgres with leases and retries
- impose concurrency limits per role
- queue work across sources and collections
- expose a stable HTTP/admin surface
- maintain durable completion and idempotency state
- poll sources continuously for years without relying on many short-lived processes

## Source Model

Each source plugin should answer these questions:

- what collections exist?
- what is the completion unit?
- how do we dedupe completed work?
- what tasks exist for a collection?
- what metadata documents describe the source?

That is why `SourceCollection` now includes:

- `completion`
- `task_blueprints`
- `default_poll_interval_seconds`

This keeps source semantics in Rust rather than leaking them into orchestration.

## Control Plane Responsibilities

The services own:

- source registry
- collection registry
- discovery cursors
- artifact completion tracking
- task queueing
- worker concurrency limits
- schema registry updates
- raw-table reconciliation
- semantic-view reconciliation
- promotion scheduling

Postgres is the right operational home because the system needs multiple workers with `SKIP LOCKED` claims, advisory-lock leadership, and durable retry state.

## Warehouse Responsibilities

ClickHouse owns:

- schema-hash-specific raw physical tables
- stable semantic views over compatible raw schemas
- curated serving datasets for portals and LLMs
- analytical copies of control-plane events when useful

This follows the `nemscraper` pattern, but formalizes it:

- safe raw storage by schema hash
- stable query surface above it
- explicit control-plane state outside the warehouse

## Dagster Role

Dagster is optional.

If retained, it should:

- trigger service actions
- display freshness and lineage
- provide backfill tooling
- surface asset health

It should not own:

- task claims
- dedupe/completion truth
- schema reconciliation state
- download history

## Initial HTTP Surface

Each service exposes:

- `/healthz`
- `/livez`
- `/readyz`
- `/startupz`

That is intentionally small. The next useful endpoints are:

- `POST /tasks/discover`
- `POST /tasks/fetch`
- `POST /tasks/parse`
- `POST /tasks/reconcile/raw`
- `POST /tasks/reconcile/views`
- `POST /tasks/promote`
- `GET /artifacts`
- `GET /schemas`
- `GET /tasks`

## Data Source Inventory

The first-class source families for the service are:

- `aemo.nemweb`
  - current reports
  - archive reports
  - MMSDM-style report families
- `aemo.metadata`
  - data model reports
  - table-to-report relationships
  - population dates
  - release/upgrade metadata
- `aemo.dvd`
  - historical media and deep backfill

The service should treat metadata sources as authoritative inputs, not just documentation.

## Immediate Next Steps

1. replace the current coarse `ingest` command with service tasks
2. implement SQLite-backed task enqueue/claim/finish flows
3. move NEMweb fetch/parse into service workers
4. create raw schema-hash-specific ClickHouse tables automatically
5. generate semantic views from compatibility mappings
