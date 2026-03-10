# Energy Market Historisation Service: Technology and Hosting Options

## Executive Summary

For a 1-5 TB analytical history store focused on energy-market data such as AEMO NEMweb, the best default architecture is:

- `ClickHouse` as the primary serving warehouse for user-facing analytics and LLM-generated SQL.
- `Object storage` as the raw landing/archive layer for original files and replayable ingestion.
- `Dagster` or `Prefect` for orchestration, with a strong bias toward `Dagster` if you want asset lineage and data-product semantics built into the platform.
- `dbt` for semantic modeling, documentation, tests, and reproducible transformations.
- `OpenMetadata` for business metadata, glossary, lineage, ownership, and AI/MCP access to schema context.

### Recommended deployment choices

If the goal is best cost/performance with acceptable operational ownership:

- `Hetzner dedicated server + Hetzner Object Storage + self-managed ClickHouse`

If the goal is lowest operational burden and fastest route to production:

- `ClickHouse Cloud (or BYOC) + S3 + managed orchestration`

If the goal is maximum SQL familiarity and lowest conceptual complexity for the team:

- `PostgreSQL/Timescale` is viable, but not my first recommendation for the main 1-5 TB portal-serving warehouse.

## Why This Recommendation

Your workload is much closer to `analytical event history` than `transaction processing`:

- large append-heavy datasets
- frequent filtering by time, market, region, asset, report type, and interval
- many scans and aggregations
- likely need for materialized rollups
- likely future need to join several market data families
- portal traffic where user experience matters
- LLM-generated queries that will be imperfect and benefit from fast columnar reads

That points strongly toward a columnar analytical database, not a row-store-first design.

## Workload Assumptions

These assumptions drive the recommendations:

- Data volume reaches `1-5 TB` compressed or near-compressed warehouse scale.
- First source is `NEMweb`, but more ISO/RTO/market or device sources will be added later.
- Historical replay and reprocessing matter.
- Most writes are append or late-arriving corrections, not heavy OLTP updates.
- Query patterns are mostly analytical: time-window filters, grouped aggregates, dimensional joins, comparisons, anomaly slices, and trend analysis.
- The portal will need both:
  - human-facing low-latency queries
  - machine-facing schema metadata that LLMs can inspect safely and consistently

## NEMweb-Specific Implications

NEMweb has some characteristics that should shape the design:

- Files are published as `CSV` and archived `ZIP` bundles.
- The CSV framework uses record markers such as `D` rows for payload data rather than plain header-only CSV.
- AEMO provides a formal market data model and documentation around tables, indexes, and primary keys.
- Historical archives and current reports should be preserved exactly so you can replay parsers when AEMO changes formats or you improve normalization logic.

Implication: do not ingest NEMweb straight into only one final warehouse table set. Keep a replayable raw layer.

## Evaluation Criteria

I evaluated options against:

- ingest performance
- analytical query speed
- storage efficiency
- operational complexity
- schema evolution
- ability to add new scrapers cleanly
- metadata clarity for LLMs
- total cost of ownership
- ease of self-hosting versus managed hosting

## Shortlist

### Option 1: ClickHouse + Object Storage + Dagster + dbt + OpenMetadata

This is the strongest overall fit.

#### Pros

- Excellent scan and aggregation performance on large time-series/event data.
- Strong compression and efficient storage for append-heavy analytical workloads.
- Very good fit for pre-aggregations and serving tables through materialized views.
- Works well with object storage and staged ingestion patterns.
- Good fit for LLM-generated SQL because badly-shaped analytical queries are still often tolerable compared with row stores.
- Easy to separate raw, normalized, and serving layers inside one warehouse.

#### Cons

- Operational model is less familiar than PostgreSQL.
- Data mutation/update semantics are more specialized than OLTP systems.
- You need discipline around table ordering keys, partitions, and materialized views.

#### Best use here

- Portal-serving warehouse.
- Long historical storage with fast time-window analytics.
- High-cardinality event and measurement data.
- Mixed raw + summary table strategy.

### Option 2: PostgreSQL + Timescale + dbt + OpenMetadata

This is the best simpler alternative, but I would treat it as the "SQL familiarity first" choice rather than the best long-term analytical engine.

#### Pros

- PostgreSQL ecosystem is mature, familiar, and easier to hire for.
- Good relational semantics, strong metadata patterns, and straightforward joins.
- Timescale adds hypertables, compression/columnstore, and continuous aggregates.
- Easier for complex transactional metadata and app-side integrations.

#### Cons

- At 1-5 TB, cost/performance can be materially worse than ClickHouse for broad analytical scans.
- You will work harder on partitioning, indexing, and storage tuning.
- LLM-generated exploratory queries are more likely to stress the system.
- Managed PostgreSQL at this scale often gets expensive quickly.

#### Best use here

- Team is strongly PostgreSQL-centric.
- Query patterns are more relational than large-scan analytical.
- You want one database for metadata and analytics and accept lower analytical efficiency.

### Option 3: Iceberg Lakehouse + Object Storage + Query Engine

This is the most open and future-proof architecture, but it is not the best first system unless multi-engine interoperability is a top requirement now.

Typical stack:

- `Apache Iceberg` tables on object storage
- query engine such as `Trino`, `Spark`, or increasingly `ClickHouse` reading lakehouse formats
- orchestration + metadata catalog

#### Pros

- Open table format with schema evolution, partition evolution, and time travel.
- Strong long-term interoperability.
- Good if you expect multiple compute engines and heavy data engineering workflows.
- Useful if you want warehouse data to remain portable across vendors.

#### Cons

- Higher platform complexity.
- More moving parts before you get a fast, stable portal experience.
- Operationally heavier than a single serving warehouse.
- Usually overkill for the first production iteration.

#### Best use here

- You know you want a multi-engine lakehouse from day one.
- You expect Spark/Flink/Trino style data engineering alongside serving workloads.
- Vendor portability matters more than initial simplicity.

## Decision Table

| Option | Performance for portal queries | Cost efficiency | Operational complexity | Metadata/LLM friendliness | Extensibility | Overall fit |
|---|---|---:|---:|---:|---:|---:|
| ClickHouse stack | Excellent | Excellent to very good | Medium | Very good | Excellent | Best |
| Postgres/Timescale stack | Good | Fair to good | Low to medium | Excellent | Good | Strong fallback |
| Iceberg lakehouse stack | Good to excellent | Good | High | Good to excellent | Excellent | Good later-stage option |

## Recommended Architecture

### Logical layers

Use four layers:

1. `raw_archive`
   - immutable original NEMweb ZIP/CSV artifacts
   - checksum, source URL, fetch time, publication time, parser version

2. `raw_parsed`
   - parser-normalized records that preserve source fidelity
   - one table family per source/report type
   - minimal type cleanup, no business reshaping yet

3. `core`
   - canonical conformed entities and facts
   - consistent keys, dimensions, temporal validity, source provenance

4. `serving`
   - pre-aggregated marts and user-facing tables/views optimized for portal usage

This separation gives you:

- replayability
- safe parser evolution
- clear source lineage
- easier onboarding of future scrapers
- better LLM context because table purpose is explicit

### Ingestion pattern

Each scraper should implement the same contract:

- `discover()` -> list candidate files or API windows
- `fetch()` -> download raw artifact to object storage
- `fingerprint()` -> checksum, source metadata, duplicate detection
- `parse()` -> normalize source-specific format
- `load_raw()` -> append parsed records into raw tables
- `transform()` -> dbt models build core/serving tables
- `validate()` -> row counts, freshness, schema checks, domain checks
- `publish_metadata()` -> update catalog entries and docs

Define this as a plugin interface so NEMweb is just the first implementation.

Suggested plugin registry shape:

```python
class SourcePlugin(Protocol):
    name: str
    version: str

    def discover(self, ctx) -> list[ArtifactRef]: ...
    def fetch(self, artifact: ArtifactRef, ctx) -> LocalArtifact: ...
    def parse(self, artifact: LocalArtifact, ctx) -> Iterable[ParsedBatch]: ...
    def load_raw(self, batch: ParsedBatch, ctx) -> LoadResult: ...
    def validations(self) -> list[ValidationCheck]: ...
    def metadata(self) -> SourceMetadata: ...
```

### Warehouse design

#### If using ClickHouse

Use:

- partitioning primarily by coarse time windows such as month
- `ORDER BY` on the main query path, for example `(market, report_type, interval_time, region_id, duid)` where appropriate
- `LowCardinality(String)` for repeated low-cardinality string dimensions
- materialized views for common rollups such as:
  - 5-minute by region
  - 30-minute settlement by region
  - daily generator summaries
  - interconnector flow summaries

Keep dimension/reference tables small and clean:

- participants
- units / DUIDs
- regions
- interconnectors
- constraints
- trading intervals / dispatch intervals calendar
- source/report registry

#### If using PostgreSQL/Timescale

Use:

- hypertables on time columns
- compression/columnstore for older chunks
- BRIN indexes for large append-correlated tables
- btree indexes only on high-value predicates
- continuous aggregates for standard portal rollups

This can work well, but it will require more care as data and concurrency grow.

## Best Hosting Options

## Option A: Self-managed dedicated server + object storage

### Best fit

- strongest cost/performance
- small team willing to own infrastructure
- can tolerate self-managed backups, upgrades, and monitoring

### Recommended shape

- `Hetzner dedicated server` for ClickHouse and ingestion workers
- `Hetzner Object Storage` for raw archives and possibly cold snapshots
- optional second smaller node for backups, monitoring, and failover services

### Why it is attractive

- Bare metal or dedicated hosting is usually hard to beat on price-per-TB and price-per-core.
- You avoid managed database premiums.
- For 1-5 TB analytical workloads, local NVMe is valuable for hot query performance.

### Risks

- You own upgrades, backup verification, and failover design.
- Single-node failure domains need to be mitigated intentionally.
- Managed cloud operational conveniences are gone.

### My view

For an early-stage product with real cost sensitivity, this is a very strong option if the team is comfortable operating Linux, storage, and backups.

## Option B: ClickHouse Cloud / BYOC + object storage

### Best fit

- you want the best time-to-production
- you want strong operational offload
- you expect meaningful user traffic and want supportability

### Recommended shape

- `ClickHouse Cloud` or `ClickHouse BYOC`
- `Amazon S3` for raw landing/archive
- managed orchestrator or lightweight workers on ECS/Kubernetes/VMs
- metadata catalog hosted separately

### Why it is attractive

- Lowest operational burden for the serving warehouse.
- Strong alignment with ClickHouse's cloud-native storage/compute model.
- Easier scaling path than self-managed clusters.

### Risks

- Higher recurring cost.
- Some flexibility is traded for service boundaries and vendor model.

### My view

If the portal is strategic and user experience matters more than minimizing infra spend, this is the cleanest production choice.

## Option C: AWS-native self-managed

### Best fit

- you want mainstream cloud primitives and broad ecosystem integration
- you need US/AU-region cloud tooling and standard enterprise patterns

### Recommended shape

- EC2 for ClickHouse or PostgreSQL
- EBS/NVMe for hot storage
- S3 for raw archive
- CloudWatch/Prometheus/Grafana for monitoring
- ECS or simple EC2 workers for scrapers/orchestration

### Why it is attractive

- Operational flexibility
- standard IAM/networking/secrets model
- easier enterprise integration later

### Risks

- At this scale, AWS can be materially more expensive than a dedicated server strategy.
- You still own most database operations unless you move to managed services.

### My view

Good if you want cloud-native patterns, but not the cheapest path.

## Option D: Managed PostgreSQL / Tiger Cloud

### Best fit

- simplicity and PostgreSQL familiarity matter most
- analytical workload is moderate rather than extreme

### My view

This is the most comfortable path, but probably not the best economics or performance profile for the portal if the historical warehouse becomes the analytical backbone.

## Recommended Hosting Decision

### Best cost/performance

- `Hetzner dedicated + self-managed ClickHouse + Hetzner Object Storage`

### Best managed/production-ready path

- `ClickHouse Cloud/BYOC + S3`

### Best if team insists on PostgreSQL

- `Tiger Cloud` or carefully self-managed PostgreSQL/Timescale

## Metadata and Schema Strategy for LLMs

This is critical. Do not rely on raw table names and column names alone.

Use two layers of metadata:

- `in-database metadata tables` that the portal and LLM can query cheaply
- `external metadata catalog` for lineage, glossary, ownership, search, and AI access

### In-database metadata schema

Create a dedicated schema such as `meta` with tables like:

- `meta.datasets`
- `meta.tables`
- `meta.columns`
- `meta.relationships`
- `meta.metrics`
- `meta.business_terms`
- `meta.source_artifacts`
- `meta.freshness`
- `meta.data_quality_results`

Each table in the analytical warehouse should have metadata fields or linked metadata rows containing:

- human-readable description
- grain
- primary business key
- time semantics
- source system
- update cadence
- nullability expectations
- data quality notes
- common filters
- common joins
- example questions this table can answer
- anti-patterns and things the table should not be used for

Example useful columns:

```sql
meta.tables(
  table_name,
  layer,
  domain,
  description,
  grain,
  entity_type,
  primary_time_column,
  primary_business_key,
  update_cadence,
  retention_policy,
  owner,
  llm_usage_notes,
  example_queries_md,
  joins_md
)
```

### Relationship metadata

LLMs need explicit join guidance. Store it as data, not only docs.

For each relationship, capture:

- left table / column
- right table / column
- join type
- cardinality
- temporal validity rule
- whether the relation is hard, soft, or derived
- plain-English explanation

Example:

```sql
meta.relationships(
  left_table,
  left_column,
  right_table,
  right_column,
  relationship_type,
  cardinality,
  temporal_rule,
  confidence,
  description
)
```

### Why OpenMetadata is a strong fit

OpenMetadata is particularly relevant here because:

- it supports glossary and lineage
- it can ingest dbt lineage
- it exposes metadata to AI tooling through MCP

That matters because you explicitly want LLMs to understand table meaning and relationships, not just inspect bare schemas.

## dbt Recommendation

Use dbt even if you keep transformations modest at first.

Reasons:

- model contracts
- tests
- docs
- lineage graph
- YAML descriptions close to code
- clean promotion from raw -> core -> serving

Suggested dbt layout:

- `models/raw`
- `models/core`
- `models/serving`
- `models/semantic`
- `sources.yml`
- `schema.yml` per domain

Use dbt docs plus OpenMetadata ingestion so the same metadata is available both to humans and AI tooling.

## Query Performance Strategy for the Portal

The portal should not point LLMs at only the deepest raw tables.

Instead:

- expose curated `serving` models for common business questions
- reserve raw/core tables for expert or fallback use
- maintain pre-aggregated tables for common time grains
- apply query guardrails in the portal:
  - mandatory time filters on large fact tables
  - row limits
  - allowed join paths
  - preferred serving tables by question category

### Practical serving models to prebuild for NEMweb

- dispatch price by region and interval
- demand by region and interval
- generation by unit / fuel / region and interval
- constraint summary by interval
- FCAS summary by region and interval
- daily and monthly market summaries

### Portal pattern

Use a two-step query path:

1. first consult `meta.*` and/or OpenMetadata for table selection
2. then generate SQL against a constrained set of serving models

This is materially safer than letting the model free-form browse the warehouse.

## Concrete Recommended Stack

### Cost-optimized recommended stack

- Warehouse: `ClickHouse OSS`
- Raw archive: `Hetzner Object Storage`
- Compute: `Hetzner dedicated server`
- Orchestration: `Dagster OSS`
- Transformations: `dbt`
- Metadata/catalog: `OpenMetadata`
- Monitoring: `Grafana + Prometheus + Loki`
- Scraper runtime: `Python`

### Managed recommended stack

- Warehouse: `ClickHouse Cloud` or `BYOC`
- Raw archive: `Amazon S3`
- Orchestration: `Dagster+` or `Prefect Cloud`
- Transformations: `dbt`
- Metadata/catalog: `OpenMetadata`
- Scraper runtime: `Python` on ECS/Kubernetes/VMs

## Technology Choices I Would Avoid As The Primary First Warehouse

### Plain PostgreSQL without Timescale

Too much manual tuning burden for this workload.

### A full lakehouse-first stack without a strong reason

Too much complexity for the first version unless open-format interoperability is already a hard requirement.

### Using only the raw NEMweb relational model as the product schema

It is useful as a source reference, but not sufficient as your product-facing semantic layer. You need conformed and well-described portal-serving models.

## Suggested Rollout Plan

### Phase 1

- Stand up raw archive storage
- Implement NEMweb plugin
- Land immutable source artifacts
- Parse to raw tables
- Build first core dimensions and facts
- Build metadata schema

### Phase 2

- Add dbt models and tests
- Add serving marts for portal use cases
- Add OpenMetadata and glossary
- Add query guardrails in the portal

### Phase 3

- Add more source plugins
- Add data quality checks and freshness SLAs
- Add backfill tooling and reprocessing workflows
- Add warm/cold storage policies and retention tuning

## Bottom Line

If I were making the decision for this project today, I would choose:

- `ClickHouse` for the analytical warehouse
- `Object storage` for raw immutable source data
- `Dagster + dbt` for extensible ingestion and transformation
- `OpenMetadata` for the LLM-facing metadata and lineage layer

For hosting:

- choose `Hetzner dedicated + Hetzner Object Storage` if minimizing recurring cost is the main constraint
- choose `ClickHouse Cloud/BYOC + S3` if minimizing operational burden is the main constraint

That gives you the best combination of:

- cost efficiency
- analytical performance
- replayable ingestion
- extensibility for new scrapers
- metadata clarity for AI-assisted querying

## Sources

- AEMO NEM data and NEMweb references:
  - https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem
  - https://markets-portal-help.docs.public.aemo.com.au/Content/CSVdataFormat/0_-D_-_data_rows.htm
  - https://markets-portal-help.docs.public.aemo.com.au/Content/DataSubscription/Downloading_the_current_MMS.htm
- ClickHouse:
  - https://clickhouse.com/docs
  - https://clickhouse.com/integrations/amazon_s3
  - https://clickhouse.com/blog/using-materialized-views-in-clickhouse
  - https://clickhouse.com/pricing
- PostgreSQL:
  - https://www.postgresql.org/docs/17/ddl-partitioning.html
  - https://www.postgresql.org/docs/18/indexes-types.html
- Timescale / Tiger Data:
  - https://www.tigerdata.com/timescaledb
  - https://www.tigerdata.com/pricing
- Apache Iceberg:
  - https://iceberg.apache.org/docs/1.6.0/
  - https://iceberg.apache.org/docs/1.4.0/evolution/
- Orchestration:
  - https://docs.dagster.io/
  - https://docs.prefect.io/v3/concepts/flows
- Metadata:
  - https://docs.open-metadata.org/v1.12.x/api-reference/sdk/ai/mcp-tools
  - https://docs.open-metadata.org/v1.13.x-SNAPSHOT/connectors/database/dbt/ingest-dbt-lineage
- Hosting/storage pricing references:
  - https://www.hetzner.com/storage/object-storage/overview/
  - https://www.hetzner.com/cloud
  - https://aws.amazon.com/s3/pricing/
  - https://aws.amazon.com/ec2/pricing/on-demand/
  - https://aws.amazon.com/rds/postgresql/pricing/
