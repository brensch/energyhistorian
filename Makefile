DATA_DIR ?= ./data
CH = set -a; [ -f .env ] && . ./.env; set +a; : "$${CLICKHOUSE_ADMIN_USER:?CLICKHOUSE_ADMIN_USER is required}"; : "$${CLICKHOUSE_ADMIN_PASSWORD:?CLICKHOUSE_ADMIN_PASSWORD is required}"; docker compose exec clickhouse clickhouse-client --user "$${CLICKHOUSE_ADMIN_USER}" --password "$${CLICKHOUSE_ADMIN_PASSWORD}"

.PHONY: run run-watch ai-api-watch postgres-up clickhouse-provision-users build web-dev web-build status stats tables views ch

# ── Run ────────────────────────────────────────────────────────────
run:
	RUST_LOG=info cargo run --package energyhistorian -- --data-dir $(DATA_DIR)

run-watch:
	RUST_LOG=info cargo watch \
		-w src \
		-w crates \
		-w Cargo.toml \
		-w Cargo.lock \
		-x "run --package energyhistorian -- --data-dir $(DATA_DIR)"

ai-api-watch:
	docker compose --profile ai up -d postgres
	RUST_LOG=info cargo watch \
		-w apps/ai-api/src \
		-w apps/ai-api/Cargo.toml \
		-w Cargo.lock \
		-x "run -p ai-api"

postgres-up:
	docker compose --profile ai up -d postgres

clickhouse-provision-users:
	@set -a; \
	[ -f .env ] && . ./.env; \
	set +a; \
	/bin/sh ./scripts/provision_clickhouse_users.sh

build:
	cargo build --package energyhistorian --release

web-dev:
	@set -a; \
	[ -f .env ] && . ./.env; \
	set +a; \
	VITE_API_BASE_URL="$${API_BASE_URL:-http://localhost:8090}" \
	VITE_WORKOS_CLIENT_ID="$${WORKOS_CLIENT_ID:-}" \
	VITE_WORKOS_API_HOSTNAME="$${WORKOS_API_HOSTNAME:-}" \
	VITE_ENABLE_DEV_AUTH="$${ALLOW_DEV_AUTH:-false}" \
	npm --prefix apps/web run dev

web-build:
	@set -a; \
	[ -f .env ] && . ./.env; \
	set +a; \
	VITE_API_BASE_URL="$${API_BASE_URL:-http://localhost:8090}" \
	VITE_WORKOS_CLIENT_ID="$${WORKOS_CLIENT_ID:-}" \
	VITE_WORKOS_API_HOSTNAME="$${WORKOS_API_HOSTNAME:-}" \
	VITE_ENABLE_DEV_AUTH="$${ALLOW_DEV_AUTH:-false}" \
	npm --prefix apps/web run build

# ── Observability ──────────────────────────────────────────────────
status:
	@curl -s http://localhost:8080/status | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin), indent=2))"

stats:
	@$(CH) --query "SELECT database, count() AS tables, sum(total_rows) AS rows, \
		formatReadableSize(sum(total_bytes)) AS size \
		FROM system.tables \
		WHERE database LIKE 'raw_%' OR database = 'semantic' \
		GROUP BY database ORDER BY database"

tables:
	@$(CH) --query "SELECT database, name, total_rows FROM system.tables \
		WHERE (database LIKE 'raw_%' OR database = 'semantic') AND total_rows > 0 \
		ORDER BY database, total_rows DESC"

views:
	@$(CH) --query "SELECT name FROM system.tables WHERE database = 'semantic' ORDER BY name"

# ── SQLite ─────────────────────────────────────────────────────────
queue:
	@sqlite3 $(DATA_DIR)/historian.db "SELECT status, count(*) FROM artifacts GROUP BY status ORDER BY status;"

cycle:
	@printf "\n== Artifact Status ==\n"
	@sqlite3 $(DATA_DIR)/historian.db "SELECT status, count(*) FROM artifacts GROUP BY status ORDER BY status;"
	@printf "\n== Recent Parse Runs ==\n"
	@sqlite3 $(DATA_DIR)/historian.db "SELECT count(*) as total_parses, sum(row_count) as total_rows FROM parse_runs WHERE status='succeeded';"
	@printf "\n== Failed Artifacts ==\n"
	@sqlite3 $(DATA_DIR)/historian.db "SELECT artifact_id, status, substr(error_text, 1, 120) FROM artifacts WHERE status LIKE '%failed%' ORDER BY updated_at DESC LIMIT 10;"

# ── Interactive ────────────────────────────────────────────────────
ch:
	@set -a; \
	[ -f .env ] && . ./.env; \
	set +a; \
	: "$${CLICKHOUSE_ADMIN_USER:?CLICKHOUSE_ADMIN_USER is required}"; \
	: "$${CLICKHOUSE_ADMIN_PASSWORD:?CLICKHOUSE_ADMIN_PASSWORD is required}"; \
	docker compose exec -it clickhouse clickhouse-client --user "$${CLICKHOUSE_ADMIN_USER}" --password "$${CLICKHOUSE_ADMIN_PASSWORD}"

sql:
	@$(CH) --query "$(Q)"
