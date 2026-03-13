use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use rusqlite::Connection;
use tokio::sync::Mutex;

pub type Db = Arc<Mutex<Connection>>;

const BOOTSTRAP_SQL: &str = r#"
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 5000;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS schedules (
    source_id TEXT NOT NULL,
    collection_id TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    poll_interval_seconds INTEGER NOT NULL,
    next_discovery_at TEXT NOT NULL,
    last_discovery_at TEXT,
    last_success_at TEXT,
    last_error_at TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (source_id, collection_id)
);

CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    collection_id TEXT NOT NULL,
    remote_uri TEXT NOT NULL,
    artifact_kind TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    discovered_at TEXT NOT NULL,
    publication_timestamp TEXT,
    status TEXT NOT NULL DEFAULT 'discovered',
    error_text TEXT,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_artifacts_status ON artifacts(status);
CREATE INDEX IF NOT EXISTS idx_artifacts_source_status ON artifacts(source_id, collection_id, status);

CREATE TABLE IF NOT EXISTS downloads (
    artifact_id TEXT PRIMARY KEY REFERENCES artifacts(artifact_id),
    local_path TEXT NOT NULL,
    content_sha256 TEXT,
    content_length_bytes INTEGER,
    downloaded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS parse_runs (
    artifact_id TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    status TEXT NOT NULL,
    row_count INTEGER,
    summary_json TEXT,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    error_text TEXT,
    PRIMARY KEY (artifact_id, parser_version)
);
"#;

pub fn open_database(path: &Path) -> Result<Db> {
    let conn =
        Connection::open(path).with_context(|| format!("opening SQLite at {}", path.display()))?;
    conn.execute_batch(BOOTSTRAP_SQL)
        .context("bootstrapping SQLite schema")?;
    Ok(Arc::new(Mutex::new(conn)))
}

pub fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}
