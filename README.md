# energyhistorian

Rust-first energy market historisation platform, moving toward a Kubernetes-native control plane with separated scheduler, downloader, and parser services.

## What is here

- `apps/`
  - runnable Rust services
  - `schedulerd`, `downloaderd`, `parserd`
  - `energyhistoriand` remains as the current monolith during migration
- `libs/`
  - reusable Rust libraries
  - source plugins stay here so execution topology can change without rewriting source semantics
- `deploy/helm/energyhistorian`
  - single-node `k3s` Helm chart with Postgres, MinIO, and the service deployments
- `docs/platform/k3s-control-plane.md`
  - target runtime model and migration phases
- `docs/schema-evolution-strategy.md`
  - hashed raw schema strategy and stable semantic view approach

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
- The `apps/energyhistoriand` monolith is transitional and will be carved apart into the new services over time.
