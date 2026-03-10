# Service-First Architecture

## Direction

This repo is moving away from a one-shot CLI-centric ingestion model and toward a long-lived Rust service.

The target shape is:

- `energyhistoriand`
  - always-on control-plane and execution service
- `SQLite`
  - operational truth for discovery/fetch/parse/promote coordination
- `ClickHouse`
  - raw, semantic, and serving data warehouse
- optional `Dagster`
  - external orchestration and visibility, not pipeline correctness state

## Why A Service

The service model is a better fit because it can:

- keep a single managed SQLite connection with `WAL` mode
- impose concurrency limits centrally
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

The service owns:

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

SQLite is the right initial home for this because the service is expected to be a single authoritative writer.

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

The first service scaffold exposes:

- `/healthz`
- `/sources`
- `/control-plane`

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
