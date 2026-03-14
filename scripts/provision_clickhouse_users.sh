#!/bin/sh
set -eu

: "${CLICKHOUSE_URL:=http://localhost:8123/}"
: "${CLICKHOUSE_ADMIN_URL:=${CLICKHOUSE_URL}}"
: "${CLICKHOUSE_ADMIN_USER:?CLICKHOUSE_ADMIN_USER is required}"
: "${CLICKHOUSE_ADMIN_PASSWORD:?CLICKHOUSE_ADMIN_PASSWORD is required}"

: "${HISTORIAN_CH_USER:=${CLICKHOUSE_HISTORIAN_USER:?CLICKHOUSE_HISTORIAN_USER is required}}"
: "${HISTORIAN_CH_PASSWORD:=${CLICKHOUSE_HISTORIAN_PASSWORD:?CLICKHOUSE_HISTORIAN_PASSWORD is required}}"
: "${AI_API_CH_USER:=${CLICKHOUSE_AI_API_READ_USER:?CLICKHOUSE_AI_API_READ_USER is required}}"
: "${AI_API_CH_PASSWORD:=${CLICKHOUSE_AI_API_READ_PASSWORD:?CLICKHOUSE_AI_API_READ_PASSWORD is required}}"

: "${SEMANTIC_DB:=semantic}"
: "${TRACKING_DB:=tracking}"
: "${RAW_DATABASES:=raw_aemo_nemweb_data,raw_aemo_mmsdm_data,raw_aemo_mmsdm_meta,raw_aemo_docs}"

sql_quote() {
  printf "%s" "$1" | sed "s/'/''/g"
}

validate_ident() {
  case "$1" in
    ''|*[!A-Za-z0-9_]*)
      printf "invalid identifier: %s\n" "$1" >&2
      exit 1
      ;;
  esac
}

ch_query() {
  curl -fsS \
    --user "${CLICKHOUSE_ADMIN_USER}:${CLICKHOUSE_ADMIN_PASSWORD}" \
    --data-binary "$1" \
    "$CLICKHOUSE_ADMIN_URL" >/dev/null
}

grant_historian_db() {
  db="$1"
  validate_ident "$db"
  ch_query "CREATE DATABASE IF NOT EXISTS ${db}"
  ch_query "GRANT SELECT, INSERT, ALTER TABLE, CREATE TABLE, DROP TABLE, CREATE VIEW, DROP VIEW ON ${db}.* TO ${HISTORIAN_CH_USER}"
}

grant_ai_api_db() {
  db="$1"
  validate_ident "$db"
  ch_query "CREATE DATABASE IF NOT EXISTS ${db}"
  ch_query "GRANT SELECT ON ${db}.* TO ${AI_API_CH_USER}"
}

validate_ident "$HISTORIAN_CH_USER"
validate_ident "$AI_API_CH_USER"
validate_ident "$SEMANTIC_DB"
validate_ident "$TRACKING_DB"

hist_pw="$(sql_quote "$HISTORIAN_CH_PASSWORD")"
ai_pw="$(sql_quote "$AI_API_CH_PASSWORD")"

ch_query "CREATE USER IF NOT EXISTS ${HISTORIAN_CH_USER} IDENTIFIED BY '${hist_pw}'"
ch_query "ALTER USER ${HISTORIAN_CH_USER} IDENTIFIED BY '${hist_pw}'"
ch_query "ALTER USER ${HISTORIAN_CH_USER} SETTINGS readonly = 0"
ch_query "GRANT CREATE DATABASE ON *.* TO ${HISTORIAN_CH_USER}"
ch_query "GRANT SELECT ON system.columns TO ${HISTORIAN_CH_USER}"
ch_query "GRANT SELECT ON system.tables TO ${HISTORIAN_CH_USER}"

ch_query "CREATE USER IF NOT EXISTS ${AI_API_CH_USER} IDENTIFIED BY '${ai_pw}'"
ch_query "ALTER USER ${AI_API_CH_USER} IDENTIFIED BY '${ai_pw}'"
ch_query "ALTER USER ${AI_API_CH_USER} SETTINGS readonly = 1"
ch_query "GRANT SELECT ON system.columns TO ${AI_API_CH_USER}"
ch_query "GRANT SELECT ON system.tables TO ${AI_API_CH_USER}"

OLD_IFS="$IFS"
IFS=','
for db in $RAW_DATABASES; do
  db="$(printf "%s" "$db" | xargs)"
  [ -n "$db" ] || continue
  grant_historian_db "$db"
  grant_ai_api_db "$db"
done
IFS="$OLD_IFS"

grant_historian_db "$SEMANTIC_DB"
grant_historian_db "$TRACKING_DB"
grant_ai_api_db "$SEMANTIC_DB"

printf "Provisioned ClickHouse users:\n" >&2
printf "  historian: %s\n" "$HISTORIAN_CH_USER" >&2
printf "  ai-api readonly: %s\n" "$AI_API_CH_USER" >&2
