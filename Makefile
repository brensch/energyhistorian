CLICKHOUSE_URL ?= http://127.0.0.1:8123
CLICKHOUSE_USER ?= energyhistorian
CLICKHOUSE_PASSWORD ?= energyhistorian

.PHONY: stats

stats:
	@curl -fsS "$(CLICKHOUSE_URL)/?user=$(CLICKHOUSE_USER)&password=$(CLICKHOUSE_PASSWORD)" \
		--data-binary "WITH table_rows AS (SELECT t.database AS database, t.name AS table, coalesce(p.row_count, 0) AS row_count FROM system.tables t LEFT JOIN (SELECT database, table, sum(rows) AS row_count FROM system.parts WHERE active GROUP BY database, table) p ON p.database = t.database AND p.table = t.name WHERE t.database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')) SELECT database, table, row_count FROM table_rows ORDER BY database, table FORMAT PrettyCompact"; \
	echo; \
	curl -fsS "$(CLICKHOUSE_URL)/?user=$(CLICKHOUSE_USER)&password=$(CLICKHOUSE_PASSWORD)" \
		--data-binary "SELECT sum(rows) AS total_rows FROM system.parts WHERE active AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') FORMAT PrettyCompact"
