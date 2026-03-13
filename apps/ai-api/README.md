# ai-api

Rust chat backend for the NEM semantic question-answering flow.

## What it does

- authenticates users with WorkOS JWTs
- stores users, orgs, conversations, runs, usage, and subscriptions in Postgres
- plans read-only `semantic.*` SQL against ClickHouse
- validates and repairs SQL before execution
- streams chat run status over SSE
- creates Stripe checkout and portal sessions

## API

- `GET /healthz`
- `GET /readyz`
- `GET /v1/me`
- `GET /v1/conversations`
- `GET /v1/conversations/:id`
- `GET /v1/usage`
- `POST /v1/chat/stream`
- `POST /v1/billing/checkout`
- `POST /v1/billing/portal`
- `POST /v1/webhooks/stripe`

`POST /v1/chat/stream` expects:

```json
{
  "question": "What was the average NSW1 spot price over the last 7 days?",
  "conversation_id": null,
  "approved_proposal": null
}
```

It returns Server-Sent Events with payloads like:

- `run_started`
- `status`
- `plan`
- `sql`
- `query_preview`
- `answer`
- `completed`
- `failed`

## Environment

Required:

- `DATABASE_URL`
- `CLICKHOUSE_READ_URL`
- `CLICKHOUSE_WRITE_URL`
- `WORKOS_ISSUER`
- `WORKOS_AUDIENCE`
- one of:
  - `LLM_PROVIDER=openai` and `OPENAI_API_KEY`
  - `LLM_PROVIDER=anthropic` and `ANTHROPIC_API_KEY`

Optional:

- `AI_API_PORT` default `8090`
- `CLICKHOUSE_VIEW_DB` default `semantic`
- `CLICKHOUSE_USAGE_DB` default `tracking`
- `ALLOW_DEV_AUTH=true`
- `ADMIN_EMAILS=a@example.com,b@example.com`
- Stripe:
  - `STRIPE_SECRET_KEY`
  - `STRIPE_WEBHOOK_SECRET`
  - `STRIPE_PRICE_ID`
  - `STRIPE_SUCCESS_URL`
  - `STRIPE_CANCEL_URL`
  - `STRIPE_PORTAL_RETURN_URL`

## Local run

`CLICKHOUSE_READ_URL` must point to a ClickHouse user with `readonly=1` or `readonly=2`.
The service validates that on startup and refuses to boot if the read DSN is write-capable.
If your semantic views resolve to underlying `raw_*` tables, that readonly user also needs
`SELECT` on those backing databases. The API still validates generated SQL so LLM-issued
queries remain constrained to `semantic.*` objects.

Bring up ClickHouse, Postgres, and the API:

```bash
docker compose --profile ai up --build postgres clickhouse ai-api
```

Or run the service directly:

```bash
cargo run -p ai-api
```

With `ALLOW_DEV_AUTH=true`, send dev headers instead of a bearer token:

```bash
curl -N http://127.0.0.1:8090/v1/chat/stream \
  -H 'Content-Type: application/json' \
  -H 'X-Dev-User-Id: dev-user' \
  -H 'X-Dev-User-Email: dev@example.com' \
  -H 'X-Dev-Org-Id: dev-org' \
  -d '{"question":"What was the average NSW1 spot price over the last 7 days?"}'
```

## GCP shape

This service is a good Cloud Run target.

- run `ai-api` on Cloud Run
- use Cloud SQL Postgres for `DATABASE_URL`
- keep ClickHouse external or privately networked
- terminate auth at WorkOS-issued bearer tokens
- use Stripe webhooks against the public Cloud Run URL
