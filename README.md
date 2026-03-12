# energyhistorian

Rust-first energy market historisation platform, moving toward a Kubernetes-native control plane with separated scheduler, downloader, and parser services.

## What is here

- `apps/`
  - runnable Rust services
  - `schedulerd`, `downloaderd`, `parserd`
- `libs/`
  - reusable Rust libraries
  - source plugins stay here so execution topology can change without rewriting source semantics
- `deploy/helm/energyhistorian`
  - single-node `k3s` Helm chart with Postgres, MinIO, and the service deployments
- `docs/platform/k3s-control-plane.md`
  - target runtime model and migration phases
- `docs/schema-evolution-strategy.md`
  - hashed raw schema strategy and stable semantic view approach

## Architecture

### Kubernetes runtime

The runtime is split into three long-running services backed by Postgres for task coordination, MinIO for immutable fetched artifacts, and ClickHouse for raw and semantic analytics.

- `schedulerd`
  - owns collection schedules and enqueues `discover` work
- `downloaderd`
  - claims `fetch` tasks, downloads remote artifacts, and stores them in MinIO
- `parserd`
  - claims `parse` tasks, reads artifacts from MinIO, publishes raw tables into ClickHouse, and then reconciles semantic views for the affected source

Current local Helm defaults:

- scheduler replicas: `1`
- downloader replicas: `1`
- parser replicas: `3`

```mermaid
flowchart TB
    Sources["AEMO / plugin sources"]

    subgraph K8s["Kubernetes cluster"]
        direction LR

        subgraph Services["Services"]
            direction TB
            Scheduler["schedulerd\n1 replica"]
            Downloader["downloaderd\n1 replica"]
            Parser["parserd\n3 replicas"]
            Reconciler["semantic reconciliation"]
        end

        subgraph State["Stateful services"]
            direction TB
            Postgres[("Postgres\ncontrol plane")]
            MinIO[("MinIO\nartifact store")]
            ClickHouse[("ClickHouse\nraw + semantic warehouse")]
        end
    end

    Scheduler -->|discover via HTTP| Sources
    Downloader -->|download artifacts| Sources

    Scheduler -->|enqueue and claim discover work| Postgres
    Downloader -->|claim fetch work| Postgres
    Parser -->|claim parse work| Postgres

    Downloader -->|write fetched files| MinIO
    Parser -->|read fetched files| MinIO
    Parser -->|publish raw_* tables| ClickHouse
    Parser --> Reconciler
    Reconciler -->|create or update semantic.*| ClickHouse
```

### Warehouse shape

The warehouse has two main layers:

- `raw_*`
  - append-only physical tables published directly from parsed artifacts
  - schema-hash variants are kept separate
- `semantic.*`
  - code-defined views and optional ETL jobs owned by each plugin
  - consolidates raw schema variants into stable query surfaces
  - examples:
    - NEMWEB logical views such as `semantic.daily_unit_dispatch`
    - MMSDM current-state dimension such as `semantic.unit_dimension`
    - `semantic.semantic_model_registry` for LLM-facing metadata

The semantic jobs are defined by source plugins and executed either:

- automatically after successful parse runs
- manually via `apps/reconcile-semantics`

```mermaid
flowchart LR
    subgraph Inputs["Inputs"]
        direction TB
        Raw["raw_<source>.*\nphysical schema-hash tables"]
        Observed["observed_schemas\nlogical table metadata"]
    end

    Jobs["plugin semantic jobs\nConsolidateObservedSchemaViews\nSqlView\nSqlTable"]

    subgraph Outputs["Outputs"]
        direction TB
        Semantic["semantic.*\nstable query surfaces"]
        Registry["semantic_model_registry\nobject descriptions, grain, joins, caveats"]
    end

    Raw --> Jobs
    Observed --> Jobs
    Jobs --> Semantic
    Jobs --> Registry
```

### Question answering and report generation

`scripts/ask_nem.py` is the current dynamic answering harness. It treats the semantic layer as the allowed analytical surface, uses an OpenAI model to plan read-only SQL, executes the query in ClickHouse, and emits a report bundle.

The process is:

1. Load `.env`, connect to ClickHouse, and read `semantic.semantic_model_registry`
2. Ask the model to decide whether the question is answerable from `semantic.*`
3. Validate that the proposed SQL is read-only and only references allowed semantic objects
4. Run `EXPLAIN SYNTAX`, then execute the SQL in ClickHouse
5. If the query errors or returns no rows, retry with a repaired plan
6. Ask the model for an `answer`, `note`, and chart spec
7. Render a Plotly chart and write:
   - `answer.json`
   - `query.sql`
   - `results.csv`
   - `chart.html`
   - `chart.png`
   - `report.md`
   - `report.pdf`

```mermaid
flowchart TB
    User["User question"] --> Ask["scripts/ask_nem.py"]

    subgraph Planning["Planning"]
        direction LR
        Registry["semantic.semantic_model_registry"]
        Planner["OpenAI planner\nquestion -> SQL plan"]
        Validator["SQL validator\nSELECT-only\nsemantic.* only"]
    end

    subgraph Execution["Execution"]
        direction LR
        ClickHouse[("ClickHouse\nsemantic.*")]
        Results["tabular results"]
    end

    subgraph Output["Output generation"]
        direction LR
        Refiner["OpenAI answer writer\nanswer + note"]
        ChartPlanner["OpenAI chart planner"]
        Plotly["Plotly renderer"]
        Bundle["report bundle"]
        Files["answer.json\nquery.sql\nresults.csv\nchart.html\nchart.png\nreport.md\nreport.pdf"]
    end

    Ask --> Registry
    Ask --> Planner
    Planner --> Validator
    Validator --> ClickHouse
    ClickHouse --> Results
    Results --> Refiner
    Results --> ChartPlanner
    ChartPlanner --> Plotly
    Refiner --> Bundle
    Plotly --> Bundle
    Bundle --> Files
```

### Why the semantic registry exists

The registry is intentionally small. It is not meant to replace the semantic layer; it tells the answering harness just enough to use the semantic layer safely:

- stable object name
- description
- grain
- time column
- dimensions and measures
- common join keys
- caveats
- question tags

That lets the LLM plan against `semantic.*` without reverse-engineering raw warehouse structures on every request.

## Quick start

Build the workspace:

```bash
cargo check
```

Run the local multi-service stack:

```bash
docker compose up --build
```

Service health endpoints:

```bash
curl http://127.0.0.1:18080/healthz
```

Deploy to local `k3s`:

```bash
docker build -t energyhistorian/energyhistorian:latest .
helm upgrade --install energyhistorian ./deploy/helm/energyhistorian
```

Local infrastructure endpoints:

- Postgres: `127.0.0.1:5432`
- MinIO API: `127.0.0.1:9002`
- MinIO console: `127.0.0.1:9001`
- ClickHouse HTTP: `127.0.0.1:8123`

The default dev credentials remain `energyhistorian` / `energyhistorian` for Postgres and ClickHouse, and `minio` / `minio123` for MinIO.

## Notes

- The long-term direction is service-first with Postgres as the operational control plane and S3-compatible object storage for immutable fetched artifacts.
- The source crates remain the core ingestion logic; the service split is around orchestration and execution roles.
- ClickHouse remains the analytical warehouse.
- The execution path now runs through the split services with Postgres task coordination, MinIO artifact storage, and ClickHouse publication.
