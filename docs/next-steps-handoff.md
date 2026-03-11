# Next Steps Handoff

## Immediate Goal

Turn the current split-service scaffold into the real `nemscraper` replacement:

- queue-driven
- Postgres control plane
- S3-compatible immutable artifact storage
- ClickHouse raw hashed tables
- modular source plugins
- stable semantic views above raw storage

## Current State

Already in place:

- scheduler service: [apps/schedulerd/src/main.rs](/home/brensch/energyhistorian/apps/schedulerd/src/main.rs)
- downloader service: [apps/downloaderd/src/main.rs](/home/brensch/energyhistorian/apps/downloaderd/src/main.rs)
- parser service: [apps/parserd/src/main.rs](/home/brensch/energyhistorian/apps/parserd/src/main.rs)
- runtime orchestration library: [libs/runtime/src/services.rs](/home/brensch/energyhistorian/libs/runtime/src/services.rs)
- source/task/completion model: [libs/ingest-core/src/plugin.rs](/home/brensch/energyhistorian/libs/ingest-core/src/plugin.rs)
- NEMweb source plugin with real fetch/parse helper logic: [libs/source-nemweb/src/lib.rs](/home/brensch/energyhistorian/libs/source-nemweb/src/lib.rs)
- raw hashed table planner: [libs/ingest-core/src/raw_storage.rs](/home/brensch/energyhistorian/libs/ingest-core/src/raw_storage.rs)
- architecture docs:
  - [docs/service-first-architecture.md](/home/brensch/energyhistorian/docs/service-first-architecture.md)
  - [docs/schema-evolution-strategy.md](/home/brensch/energyhistorian/docs/schema-evolution-strategy.md)

## Next Implementation Steps

1. Split NEMweb execution into queue-driven stages.
   - Move `NemwebPlugin::ingest_recent(...)` into distinct service worker functions:
   - `discover_collection`
   - `fetch_artifact`
   - `parse_artifact`
   - `register_schema`
   - `reconcile_raw_table`

2. Add explicit admin APIs over the Postgres task queue when needed.
   - current workers already use leases and retries in Postgres
   - future additions can expose:
   - `POST /tasks`
   - `POST /tasks/:id/requeue`
   - `POST /tasks/:id/cancel`
   - `GET /tasks`

3. Harden worker concurrency and lease recovery further.
   - concurrency by queue/task blueprint
   - scheduler leadership via advisory lock
   - lease expiry recovery for crashed workers

4. Wire ClickHouse reconciliation and loading.
   - use the Rust `clickhouse` crate, not ad hoc HTTP
   - create raw physical tables from `ObservedSchema` using [libs/ingest-core/src/raw_storage.rs](/home/brensch/energyhistorian/libs/ingest-core/src/raw_storage.rs)
   - load parsed rows into the schema-hash-specific raw tables

5. Persist parsed rows properly.
   - current parse result only keeps sample payloads / row counts
   - change it to emit actual row batches or batch files per `(logical_table, schema_hash)`
   - those batches should be what raw-table loaders consume

6. Add semantic view reconciliation.
   - create stable views like `sem.dispatch__price`
   - initially union only compatible raw schema variants
   - leave incompatible variants quarantined but still ingested

7. Implement metadata source fetching/parsing for real.
   - start with:
   - AEMO data model reports
   - table-to-report relationships
   - population dates
   - this should drive schema descriptions and logical-table mapping

8. Add Postgres tables beyond the current minimum.
   - `schema_versions`
   - `schema_columns`
   - `raw_table_mappings`
   - `semantic_view_mappings`
   - `artifact_outputs`
   - `worker_leases`

## Important Design Constraints

- Keep Postgres as operational truth.
- Keep ClickHouse as warehouse only.
- Keep raw tables schema-hash-specific.
- Do not collapse new schemas into existing raw tables.
- Dagster is optional and should stay outside correctness-critical flow.

## What To Ignore For Now

- Dagster integration
- polished API auth
- LLM/MCP-facing portal features
- serving models beyond a minimal semantic layer

## Good First Concrete Task

If handing off for coding, the best next change is:

- add task-heartbeat renewal for long parses in [libs/runtime/src/queue.rs](/home/brensch/energyhistorian/libs/runtime/src/queue.rs)
- then move semantic promotion into a separate promoted-data service or job tier
