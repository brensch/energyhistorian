pub const CONTROL_PLANE_BOOTSTRAP_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS task_queue (
    task_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    collection_id TEXT NOT NULL,
    task_kind TEXT NOT NULL,
    task_state TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    payload_json JSONB,
    priority INTEGER NOT NULL DEFAULT 100,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 10,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_task_queue_claim
    ON task_queue (task_state, available_at, priority, created_at);

CREATE TABLE IF NOT EXISTS collection_schedules (
    source_id TEXT NOT NULL,
    collection_id TEXT NOT NULL,
    scheduler_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    poll_interval_seconds INTEGER NOT NULL,
    next_discovery_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_discovery_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    last_error_at TIMESTAMPTZ,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_id, collection_id)
);

CREATE TABLE IF NOT EXISTS discovered_artifacts (
    artifact_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    collection_id TEXT NOT NULL,
    remote_uri TEXT NOT NULL,
    artifact_kind TEXT NOT NULL,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    publication_timestamp TIMESTAMPTZ,
    artifact_metadata_json JSONB,
    status TEXT NOT NULL DEFAULT 'discovered'
);

CREATE TABLE IF NOT EXISTS stored_artifacts (
    artifact_id TEXT PRIMARY KEY REFERENCES discovered_artifacts (artifact_id),
    object_store_bucket TEXT NOT NULL,
    object_store_key TEXT NOT NULL,
    content_type TEXT,
    content_sha256 TEXT,
    content_length_bytes BIGINT,
    etag TEXT,
    stored_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS parse_runs (
    run_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES discovered_artifacts (artifact_id),
    parser_service TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    status TEXT NOT NULL,
    row_count BIGINT,
    output_summary_json JSONB,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    error_text TEXT
);
"#;
