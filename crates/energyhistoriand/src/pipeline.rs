//! Generic artifact pipeline: recording, dedup, and task chaining.
//!
//! Source plugins produce discoveries, fetched artifacts, and parse results.
//! This module handles persisting them in SQLite and creating follow-on tasks.
//! Sources never touch SQLite directly.

use std::collections::HashSet;
use std::fmt;

use anyhow::{Context, Result};
use chrono::Utc;
use ingest_core::{FileSchemaRegistry, LocalArtifact, ParseResult};
use rusqlite::Connection;

// ---------------------------------------------------------------------------
// Input types — what sources hand up
// ---------------------------------------------------------------------------

pub struct Discovery {
    pub artifact_id: String,
    pub remote_uri: String,
}

// ---------------------------------------------------------------------------
// Outcome types — what the pipeline reports back
// ---------------------------------------------------------------------------

pub struct DiscoverOutcome {
    pub total: usize,
    pub new: usize,
    pub already_done: usize,
    pub fetch_tasks_created: usize,
}

pub struct FetchOutcome {
    pub bytes: u64,
    pub parse_task_created: bool,
}

pub struct ParseOutcome {
    pub schemas: usize,
    pub tables: usize,
    pub rows: usize,
    pub registry_inserted: usize,
    pub registry_updated: usize,
}

impl fmt::Display for DiscoverOutcome {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "found {}, {} new, {} already done, {} fetch tasks",
            self.total, self.new, self.already_done, self.fetch_tasks_created
        )
    }
}

impl fmt::Display for FetchOutcome {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} bytes, parse {}",
            self.bytes,
            if self.parse_task_created {
                "enqueued"
            } else {
                "already exists"
            }
        )
    }
}

impl fmt::Display for ParseOutcome {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} schemas, {} tables, {} rows; registry +{} ~{}",
            self.schemas, self.tables, self.rows, self.registry_inserted, self.registry_updated
        )
    }
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

/// Check whether an artifact has already been fetched.
pub fn is_fetched(conn: &Connection, artifact_id: &str) -> bool {
    conn.query_row(
        "SELECT COUNT(*) FROM fetched_artifacts WHERE artifact_id = ?1",
        rusqlite::params![artifact_id],
        |row| row.get::<_, i64>(0),
    )
    .unwrap_or(0)
        > 0
}

// ---------------------------------------------------------------------------
// Recording
// ---------------------------------------------------------------------------

/// Record discovered artifacts and enqueue fetch tasks for unfetched ones.
pub fn record_discoveries(
    conn: &Connection,
    source_id: &str,
    collection_id: &str,
    discoveries: &[Discovery],
) -> DiscoverOutcome {
    let mut new = 0;
    let mut already_done = 0;
    let mut fetch_tasks_created = 0;

    for d in discoveries {
        let inserted = conn
            .execute(
                "INSERT OR IGNORE INTO discovered_artifacts \
                 (artifact_id, source_id, collection_id, remote_uri, discovered_at, status) \
                 VALUES (?1, ?2, ?3, ?4, ?5, 'discovered')",
                rusqlite::params![
                    d.artifact_id,
                    source_id,
                    collection_id,
                    d.remote_uri,
                    Utc::now().to_rfc3339(),
                ],
            )
            .unwrap_or(0);

        if inserted == 0 {
            // Already discovered — skip if it's past the fetch stage.
            let status: Option<String> = conn
                .query_row(
                    "SELECT status FROM discovered_artifacts WHERE artifact_id = ?1",
                    rusqlite::params![d.artifact_id],
                    |row| row.get(0),
                )
                .ok();
            if matches!(status.as_deref(), Some("fetched" | "parsed")) {
                already_done += 1;
                continue;
            }
        }

        new += 1;

        if is_fetched(conn, &d.artifact_id) {
            already_done += 1;
        } else {
            let fetch_id = format!("{source_id}:{collection_id}:fetch:{}", d.artifact_id);
            let payload = serde_json::json!({
                "artifact_id": d.artifact_id,
                "remote_uri": d.remote_uri,
            });
            let created = enqueue_task(
                conn,
                &fetch_id,
                source_id,
                collection_id,
                "fetch",
                &d.artifact_id,
                &payload,
            );
            if created > 0 {
                fetch_tasks_created += 1;
            }
        }
    }

    DiscoverOutcome {
        total: discoveries.len(),
        new,
        already_done,
        fetch_tasks_created,
    }
}

/// Schedule the next discovery task after a delay.
pub fn schedule_next_discovery(
    conn: &Connection,
    source_id: &str,
    collection_id: &str,
    delay_secs: i64,
) {
    let ts = Utc::now().timestamp();
    let task_id = format!("{source_id}:{collection_id}:discover:poll-{ts}");
    let available = (Utc::now() + chrono::Duration::seconds(delay_secs)).to_rfc3339();
    enqueue_task_delayed(
        conn,
        &task_id,
        source_id,
        collection_id,
        "discover",
        &format!("poll-{ts}"),
        &available,
    );
}

/// Record a successful fetch and enqueue a parse task.
pub fn record_fetch(
    conn: &Connection,
    source_id: &str,
    collection_id: &str,
    artifact_id: &str,
    remote_uri: &str,
    local: &LocalArtifact,
) -> FetchOutcome {
    let bytes = local.metadata.content_length_bytes.unwrap_or(0);

    conn.execute(
        "INSERT OR REPLACE INTO fetched_artifacts \
         (artifact_id, local_path, content_sha256, content_length_bytes, fetched_at) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![
            artifact_id,
            local.local_path.display().to_string(),
            local.metadata.content_sha256,
            local.metadata.content_length_bytes.map(|b| b as i64),
            Utc::now().to_rfc3339(),
        ],
    )
    .ok();

    conn.execute(
        "UPDATE discovered_artifacts SET status = 'fetched' WHERE artifact_id = ?1",
        rusqlite::params![artifact_id],
    )
    .ok();

    conn.execute(
        "INSERT OR IGNORE INTO artifact_publications \
         (artifact_id, source_id, collection_id, status, created_at, updated_at) \
         VALUES (?1, ?2, ?3, 'pending', ?4, ?4)",
        rusqlite::params![
            artifact_id,
            source_id,
            collection_id,
            Utc::now().to_rfc3339(),
        ],
    )
    .ok();

    let created = enqueue_parse_task(
        conn,
        source_id,
        collection_id,
        artifact_id,
        remote_uri,
        &local.local_path.display().to_string(),
    );

    FetchOutcome {
        bytes,
        parse_task_created: created > 0,
    }
}

/// Register schemas from a parse result and update artifact status.
pub fn record_parse(
    conn: &Connection,
    source_id: &str,
    collection_id: &str,
    artifact_id: &str,
    result: &ParseResult,
    schema_registry: &FileSchemaRegistry,
) -> Result<ParseOutcome> {
    let reg = schema_registry
        .register(&result.observed_schemas)
        .context("registering schemas")?;

    let tables: HashSet<&str> = result
        .raw_outputs
        .iter()
        .map(|o| o.logical_table_key.as_str())
        .collect();
    let total_rows: usize = result.raw_outputs.iter().map(|o| o.rows.len()).sum();

    conn.execute(
        "UPDATE discovered_artifacts SET status = 'published', published_at = ?1 WHERE artifact_id = ?2",
        rusqlite::params![Utc::now().to_rfc3339(), artifact_id],
    )
    .ok();

    conn.execute(
        "INSERT INTO artifact_publications \
         (artifact_id, source_id, collection_id, status, created_at, updated_at, published_at) \
         VALUES (?1, ?2, ?3, 'published', ?4, ?4, ?4) \
         ON CONFLICT(artifact_id) DO UPDATE SET \
             status = excluded.status, \
             updated_at = excluded.updated_at, \
             published_at = excluded.published_at, \
             last_error = NULL",
        rusqlite::params![
            artifact_id,
            source_id,
            collection_id,
            Utc::now().to_rfc3339(),
        ],
    )
    .ok();

    Ok(ParseOutcome {
        schemas: result.observed_schemas.len(),
        tables: tables.len(),
        rows: total_rows,
        registry_inserted: reg.inserted,
        registry_updated: reg.updated_last_seen,
    })
}

pub fn requeue_unpublished_artifacts(conn: &Connection) -> usize {
    let mut stmt = match conn.prepare(
        "SELECT d.artifact_id, d.source_id, d.collection_id, d.remote_uri, f.local_path \
         FROM discovered_artifacts d \
         JOIN fetched_artifacts f ON f.artifact_id = d.artifact_id \
         LEFT JOIN artifact_publications p ON p.artifact_id = d.artifact_id \
         WHERE d.status IN ('fetched', 'parsed') \
           AND COALESCE(p.status, 'pending') != 'published'",
    ) {
        Ok(stmt) => stmt,
        Err(_) => return 0,
    };

    let rows = match stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
        ))
    }) {
        Ok(rows) => rows,
        Err(_) => return 0,
    };

    let mut requeued = 0;
    for row in rows.flatten() {
        let (artifact_id, source_id, collection_id, remote_uri, local_path) = row;
        requeued += enqueue_parse_task(
            conn,
            &source_id,
            &collection_id,
            &artifact_id,
            &remote_uri,
            &local_path,
        );
    }

    requeued
}

// ---------------------------------------------------------------------------
// Task helpers (private — only the pipeline creates follow-on tasks)
// ---------------------------------------------------------------------------

fn enqueue_task(
    conn: &Connection,
    task_id: &str,
    source_id: &str,
    collection_id: &str,
    task_kind: &str,
    idempotency_key: &str,
    payload: &serde_json::Value,
) -> usize {
    conn.execute(
        "INSERT OR IGNORE INTO tasks \
         (task_id, source_id, collection_id, task_kind, idempotency_key, status, payload_json) \
         VALUES (?1, ?2, ?3, ?4, ?5, 'queued', ?6)",
        rusqlite::params![
            task_id,
            source_id,
            collection_id,
            task_kind,
            idempotency_key,
            payload.to_string(),
        ],
    )
    .unwrap_or(0)
}

fn enqueue_parse_task(
    conn: &Connection,
    source_id: &str,
    collection_id: &str,
    artifact_id: &str,
    remote_uri: &str,
    local_path: &str,
) -> usize {
    let parse_id = format!("{source_id}:{collection_id}:parse:{artifact_id}");
    let payload = serde_json::json!({
        "artifact_id": artifact_id,
        "remote_uri": remote_uri,
        "local_path": local_path,
    });
    enqueue_task(
        conn,
        &parse_id,
        source_id,
        collection_id,
        "parse",
        artifact_id,
        &payload,
    )
}

fn enqueue_task_delayed(
    conn: &Connection,
    task_id: &str,
    source_id: &str,
    collection_id: &str,
    task_kind: &str,
    idempotency_key: &str,
    available_at: &str,
) -> usize {
    conn.execute(
        "INSERT OR IGNORE INTO tasks \
         (task_id, source_id, collection_id, task_kind, idempotency_key, status, available_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, 'queued', ?6)",
        rusqlite::params![
            task_id,
            source_id,
            collection_id,
            task_kind,
            idempotency_key,
            available_at
        ],
    )
    .unwrap_or(0)
}
