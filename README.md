# energyhistorian

Rust-first energy market historisation platform, moving toward a long-lived service architecture.

## What is here

- `crates/energyhistoriand`
  - Rust daemon/service scaffold with SQLite control-plane state and a small HTTP surface
- `dagster/energyhistorian_dagster`
  - early orchestration prototype; no longer the architectural center
- `docs/service-first-architecture.md`
  - target service/control-plane design
- `docs/schema-evolution-strategy.md`
  - hashed raw schema strategy and stable semantic view approach
- `docs/nemweb-starting-point.md`
  - rationale for the deliberately small first NEMweb slice

## Quick start

Run the service scaffold:

```bash
cargo run --package energyhistoriand -- --listen-addr 127.0.0.1:8080
```

Then inspect the service:

```bash
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/sources
curl http://127.0.0.1:8080/control-plane
```

Run the service with Docker:

```bash
docker compose up --build
```

Then open `http://127.0.0.1:8080/healthz`.

The local ClickHouse HTTP endpoint is exposed on `http://127.0.0.1:8123` and the default dev credentials are `energyhistorian` / `energyhistorian`.

Parsed output lands under:

- `data/raw/<source>/`
- `data/parsed/<source>/`

## Notes

- The long-term direction is service-first, with SQLite for operational state and ClickHouse for warehouse storage.
- The old `nemscraper` pattern of schema-hash-specific raw tables plus stable views is the target strategy here.
- Dagster is now optional and should sit outside the core control plane if retained.
- The old CLI crate is no longer part of the supported workspace path.
