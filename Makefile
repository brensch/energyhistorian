DATA_DIR ?= ./data
CH = docker compose exec clickhouse clickhouse-client --user energyhistorian --password energyhistorian

.PHONY: run build status stats tables views ch

# ── Run ────────────────────────────────────────────────────────────
run:
	RUST_LOG=info cargo run --package energyhistorian -- --data-dir $(DATA_DIR)

build:
	cargo build --package energyhistorian --release

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
	docker compose exec -it clickhouse clickhouse-client --user energyhistorian --password energyhistorian

sql:
	@$(CH) --query "$(Q)"
