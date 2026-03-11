# K3s Control Plane Target

## Target runtime

The intended production shape is:

- `schedulerd`
  - owns collection cadence and task creation
- `downloaderd`
  - claims download tasks, fetches source artifacts, stores them in object storage
- `parserd`
  - claims parse tasks, reads stored artifacts, emits raw rows and schema observations
- `Postgres`
  - authoritative operational control plane
- `S3-compatible object storage`
  - immutable landing zone for fetched artifacts
- `ClickHouse`
  - analytical raw/semantic/serving warehouse

For local single-box operation this should run on `k3s` with:

- local-path PVCs
- a single Postgres StatefulSet
- a single MinIO deployment
- one scheduler replica
- small downloader/parser replica pools

## Why this is the right kind of unhinged

This is not overkill if the goal is to learn Kubernetes while building a service that can grow beyond AEMO.

It gives you:

- explicit service boundaries
- a real control plane instead of in-process queue state
- replayable immutable artifact storage
- horizontal scale when download or parse work spikes
- a deployment shape that can move from one box to a larger cluster without redesign

## Repository layout

The repo is being migrated to:

- `apps/`
  - runnable binaries
- `libs/`
  - reusable Rust libraries and source plugins
- `deploy/helm/energyhistorian`
  - k3s/Helm deployment
- `docs/platform`
  - operational architecture and rollout docs

## Control-plane tables

The shared bootstrap schema now targets:

- `task_queue`
- `collection_schedules`
- `discovered_artifacts`
- `stored_artifacts`
- `parse_runs`

This is intentionally narrower than the final model. It is the minimum viable durable control plane for:

- scheduling
- downloading
- parsing
- replaying

## Local bring-up target

On a k3s node with Helm installed, the intended flow is:

```bash
docker build -t energyhistorian/energyhistorian:latest .
helm upgrade --install energyhistorian ./deploy/helm/energyhistorian
kubectl get pods
kubectl logs deploy/energyhistorian-energyhistorian-scheduler
```

## Migration phases

1. move repo layout to `apps/` and `libs/`
2. introduce shared control-plane contracts
3. stand up Postgres/MinIO/scheduler/downloader/parser in k3s
4. move task state from SQLite to Postgres
5. move fetched artifact storage from local disk to S3
6. split current monolith execution logic into scheduler/downloader/parser workers
7. keep source logic in the source libraries so operational topology can change without rewriting source semantics
