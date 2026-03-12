HELM_RELEASE ?= energyhistorian
HELM_CHART   ?= deploy/helm/energyhistorian
CH_POD       ?= $(HELM_RELEASE)-energyhistorian-clickhouse-0
PG_POD       ?= $(HELM_RELEASE)-energyhistorian-postgres-0
KIND_WORKER  ?= desktop-worker

# kubectl exec wrappers (no port-forward needed)
CH  = kubectl exec $(CH_POD) -- clickhouse-client --user energyhistorian --password energyhistorian
PG  = kubectl exec $(PG_POD) -- psql -U energyhistorian -d energyhistorian -c

# Local port-forward ports (for `make proxy`)
CH_LOCAL_HTTP  ?= 8123
CH_LOCAL_TCP   ?= 9000
PG_LOCAL       ?= 5432

.PHONY: build load deploy restart ship \
        logs logs-scheduler logs-downloader logs-parser \
        stats queue queue-mmsdm tables views schema pods sql \
        psql ch proxy proxy-ch proxy-pg

# ── Build & Deploy ──────────────────────────────────────────────

build:
	docker build -t energyhistorian/energyhistorian:latest .

load:
	docker save energyhistorian/energyhistorian:latest | \
		docker exec -i $(KIND_WORKER) ctr --namespace k8s.io images import --all-platforms -

deploy:
	helm upgrade --install $(HELM_RELEASE) $(HELM_CHART)

restart:
	kubectl rollout restart deploy/$(HELM_RELEASE)-energyhistorian-scheduler \
		deploy/$(HELM_RELEASE)-energyhistorian-downloader \
		deploy/$(HELM_RELEASE)-energyhistorian-parser

ship: build load restart
	@echo "Built, loaded into Kind, and restarted all services."

# ── Port Forwarding ────────────────────────────────────────────
# Run `make proxy` in a terminal to expose CH + PG on localhost.
# Then use clickhouse-client / psql locally.

proxy: proxy-ch proxy-pg

proxy-ch:
	kubectl port-forward svc/$(HELM_RELEASE)-energyhistorian-clickhouse $(CH_LOCAL_HTTP):8123 $(CH_LOCAL_TCP):9000

proxy-pg:
	kubectl port-forward svc/$(HELM_RELEASE)-energyhistorian-postgres $(PG_LOCAL):5432

# ── Logs ────────────────────────────────────────────────────────

logs-scheduler:
	kubectl logs deploy/$(HELM_RELEASE)-energyhistorian-scheduler -f --tail=50

logs-downloader:
	kubectl logs deploy/$(HELM_RELEASE)-energyhistorian-downloader -f --tail=50

logs-parser:
	kubectl logs deploy/$(HELM_RELEASE)-energyhistorian-parser -f --tail=50

logs:
	@kubectl logs deploy/$(HELM_RELEASE)-energyhistorian-scheduler --tail=20 2>/dev/null | grep INFO | tail -5; \
	echo "---"; \
	kubectl logs deploy/$(HELM_RELEASE)-energyhistorian-downloader --tail=20 2>/dev/null | grep INFO | tail -5; \
	echo "---"; \
	kubectl logs deploy/$(HELM_RELEASE)-energyhistorian-parser --tail=20 2>/dev/null | grep INFO | tail -5

# ── Observability ───────────────────────────────────────────────

stats:
	@$(CH) --query "SELECT database, count() AS tables, sum(total_rows) AS rows, \
		formatReadableSize(sum(total_bytes)) AS size \
		FROM system.tables \
		WHERE database LIKE 'raw_%' OR database = 'semantic' \
		GROUP BY database ORDER BY database"

queue:
	@$(PG) "SELECT task_kind, task_state, count(*) FROM task_queue GROUP BY task_kind, task_state ORDER BY task_kind, task_state"

queue-mmsdm:
	@$(PG) "SELECT task_kind, task_state, count(*) FROM task_queue WHERE source_id = 'aemo.mmsdm' GROUP BY task_kind, task_state ORDER BY task_kind, task_state"

tables:
	@$(CH) --query "SELECT database, name, total_rows FROM system.tables \
		WHERE (database LIKE 'raw_%' OR database = 'semantic') AND total_rows > 0 \
		ORDER BY database, total_rows DESC"

views:
	@$(CH) --query "SELECT name FROM system.tables WHERE database = 'semantic' ORDER BY name"

schema:
	@$(CH) --query "DESCRIBE $(TABLE)"

pods:
	@kubectl get pods

# ── Interactive Shells ──────────────────────────────────────────

psql:
	kubectl exec -it $(PG_POD) -- psql -U energyhistorian -d energyhistorian

ch:
	kubectl exec -it $(CH_POD) -- clickhouse-client --user energyhistorian --password energyhistorian

# ── Ad-hoc Queries ──────────────────────────────────────────────

sql:
	@$(CH) --query "$(Q)"
