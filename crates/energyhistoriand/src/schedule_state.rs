use chrono::Utc;
use rusqlite::Connection;
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct CollectionScheduleSeed {
    pub source_id: String,
    pub collection_id: String,
    pub poll_interval_seconds: Option<u64>,
    pub scheduler_enabled: bool,
}

#[derive(Debug, Clone)]
pub struct ScheduledTask {
    pub task_id: String,
    pub available_at: String,
}

#[derive(Debug, Serialize)]
pub struct CollectionScheduleSnapshot {
    pub source_id: String,
    pub collection_id: String,
    pub scheduler_enabled: bool,
    pub status: String,
    pub next_task_kind: Option<String>,
    pub next_run_at: Option<String>,
    pub active_task_id: Option<String>,
    pub active_task_kind: Option<String>,
    pub last_task_id: Option<String>,
    pub last_task_kind: Option<String>,
    pub last_run_started_at: Option<String>,
    pub last_run_finished_at: Option<String>,
    pub last_success_at: Option<String>,
    pub last_error_at: Option<String>,
    pub last_error: Option<String>,
    pub consecutive_failures: i64,
    pub queued_tasks: i64,
    pub running_tasks: i64,
    pub failed_tasks: i64,
}

pub fn sync_collection_schedules(conn: &Connection, seeds: &[CollectionScheduleSeed]) -> usize {
    let now = Utc::now().to_rfc3339();
    let mut changed = 0;

    for seed in seeds {
        changed += conn
            .execute(
                "INSERT INTO source_collections (source_id, collection_id, updated_at) \
                 VALUES (?1, ?2, ?3) \
                 ON CONFLICT(source_id, collection_id) DO UPDATE SET updated_at = excluded.updated_at",
                rusqlite::params![seed.source_id, seed.collection_id, now],
            )
            .unwrap_or(0);

        conn.execute(
            "INSERT OR IGNORE INTO collection_schedule_state \
             (source_id, collection_id, scheduler_enabled, status, next_task_kind, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                seed.source_id,
                seed.collection_id,
                seed.scheduler_enabled as i64,
                if seed.scheduler_enabled {
                    "idle"
                } else {
                    "not_implemented"
                },
                if seed.scheduler_enabled {
                    Some("discover")
                } else {
                    None
                },
                now,
            ],
        )
        .unwrap_or(0);

        changed += conn
            .execute(
                "UPDATE collection_schedule_state \
                 SET scheduler_enabled = ?1, \
                     status = CASE \
                         WHEN ?1 = 0 THEN 'not_implemented' \
                         WHEN status = 'not_implemented' THEN 'idle' \
                         ELSE status \
                     END, \
                     next_task_kind = CASE \
                         WHEN ?1 = 0 THEN NULL \
                         WHEN next_task_kind IS NULL THEN 'discover' \
                         ELSE next_task_kind \
                     END, \
                     updated_at = ?2 \
                 WHERE source_id = ?3 AND collection_id = ?4",
                rusqlite::params![
                    seed.scheduler_enabled as i64,
                    now,
                    seed.source_id,
                    seed.collection_id,
                ],
            )
            .unwrap_or(0);
    }

    changed
}

pub fn bootstrap_collection_discovery_tasks(
    conn: &Connection,
    seeds: &[CollectionScheduleSeed],
) -> usize {
    let now = Utc::now().to_rfc3339();
    let mut bootstrapped = 0;

    for seed in seeds.iter().filter(|seed| seed.scheduler_enabled) {
        let discover_inflight = conn
            .query_row(
                "SELECT COUNT(*) FROM tasks \
                 WHERE source_id = ?1 AND collection_id = ?2 AND task_kind = 'discover' \
                   AND status IN ('queued', 'running')",
                rusqlite::params![seed.source_id, seed.collection_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0);

        if discover_inflight > 0 {
            continue;
        }

        let state: (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT last_success_at, next_run_at \
                 FROM collection_schedule_state \
                 WHERE source_id = ?1 AND collection_id = ?2",
                rusqlite::params![seed.source_id, seed.collection_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap_or((None, None));

        if state.0.is_some() || state.1.is_some() {
            continue;
        }

        let task_id = format!(
            "{}:{}:discover:bootstrap",
            seed.source_id, seed.collection_id
        );
        let inserted = conn
            .execute(
                "INSERT OR IGNORE INTO tasks \
                 (task_id, source_id, collection_id, task_kind, idempotency_key, status, available_at) \
                 VALUES (?1, ?2, ?3, 'discover', 'bootstrap', 'queued', ?4)",
                rusqlite::params![task_id, seed.source_id, seed.collection_id, now],
            )
            .unwrap_or(0);

        if inserted > 0 {
            bootstrapped += 1;
            note_discover_scheduled(
                conn,
                &seed.source_id,
                &seed.collection_id,
                &ScheduledTask {
                    task_id,
                    available_at: now.clone(),
                },
            );
        }
    }

    bootstrapped
}

pub fn note_discover_scheduled(
    conn: &Connection,
    source_id: &str,
    collection_id: &str,
    scheduled: &ScheduledTask,
) {
    let now = Utc::now().to_rfc3339();
    conn.execute(
        "UPDATE collection_schedule_state \
         SET status = 'scheduled', \
             next_task_kind = 'discover', \
             next_run_at = ?1, \
             last_task_id = ?2, \
             last_task_kind = 'discover', \
             updated_at = ?3 \
         WHERE source_id = ?4 AND collection_id = ?5",
        rusqlite::params![
            scheduled.available_at,
            scheduled.task_id,
            now,
            source_id,
            collection_id,
        ],
    )
    .ok();
}

pub fn note_task_started(
    conn: &Connection,
    source_id: &str,
    collection_id: &str,
    task_id: &str,
    task_kind: &str,
) {
    let now = Utc::now().to_rfc3339();
    conn.execute(
        "UPDATE collection_schedule_state \
         SET status = 'running', \
             active_task_id = ?1, \
             active_task_kind = ?2, \
             last_task_id = ?1, \
             last_task_kind = ?2, \
             last_run_started_at = ?3, \
             next_run_at = CASE WHEN ?2 = 'discover' THEN NULL ELSE next_run_at END, \
             updated_at = ?3 \
         WHERE source_id = ?4 AND collection_id = ?5",
        rusqlite::params![task_id, task_kind, now, source_id, collection_id],
    )
    .ok();
}

pub fn note_task_completed(conn: &Connection, source_id: &str, collection_id: &str) {
    let now = Utc::now().to_rfc3339();
    let next_run_at: Option<String> = conn
        .query_row(
            "SELECT next_run_at FROM collection_schedule_state \
             WHERE source_id = ?1 AND collection_id = ?2",
            rusqlite::params![source_id, collection_id],
            |row| row.get(0),
        )
        .ok()
        .flatten();

    let status = if next_run_at.is_some() {
        "scheduled"
    } else {
        "idle"
    };

    conn.execute(
        "UPDATE collection_schedule_state \
         SET status = ?1, \
             active_task_id = NULL, \
             active_task_kind = NULL, \
             last_run_finished_at = ?2, \
             last_success_at = ?2, \
             last_error_at = NULL, \
             last_error = NULL, \
             consecutive_failures = 0, \
             updated_at = ?2 \
         WHERE source_id = ?3 AND collection_id = ?4",
        rusqlite::params![status, now, source_id, collection_id],
    )
    .ok();
}

pub fn note_task_failed(
    conn: &Connection,
    source_id: &str,
    collection_id: &str,
    task_kind: &str,
    message: &str,
    retry_at: Option<&str>,
) {
    let now = Utc::now().to_rfc3339();
    let status = if retry_at.is_some() {
        "retry_wait"
    } else {
        "error"
    };

    conn.execute(
        "UPDATE collection_schedule_state \
         SET status = ?1, \
             active_task_id = NULL, \
             active_task_kind = NULL, \
             next_task_kind = CASE WHEN ?2 = 'discover' OR ?3 IS NOT NULL THEN ?2 ELSE next_task_kind END, \
             next_run_at = COALESCE(?3, next_run_at), \
             last_run_finished_at = ?4, \
             last_error_at = ?4, \
             last_error = ?5, \
             consecutive_failures = consecutive_failures + 1, \
             updated_at = ?4 \
         WHERE source_id = ?6 AND collection_id = ?7",
        rusqlite::params![
            status,
            task_kind,
            retry_at,
            now,
            message,
            source_id,
            collection_id,
        ],
    )
    .ok();
}

pub fn note_lease_recovered(
    conn: &Connection,
    source_id: &str,
    collection_id: &str,
    task_kind: &str,
) {
    let now = Utc::now().to_rfc3339();
    conn.execute(
        "UPDATE collection_schedule_state \
         SET status = CASE \
                 WHEN next_run_at IS NOT NULL THEN 'scheduled' \
                 ELSE 'idle' \
             END, \
             active_task_id = NULL, \
             active_task_kind = NULL, \
             next_task_kind = COALESCE(next_task_kind, ?1), \
             updated_at = ?2 \
         WHERE source_id = ?3 AND collection_id = ?4",
        rusqlite::params![task_kind, now, source_id, collection_id],
    )
    .ok();
}

pub fn list_collection_schedules(conn: &Connection) -> Vec<CollectionScheduleSnapshot> {
    let mut stmt = match conn.prepare(
        "SELECT s.source_id, s.collection_id, s.scheduler_enabled, s.status, s.next_task_kind, \
                s.next_run_at, s.active_task_id, s.active_task_kind, s.last_task_id, \
                s.last_task_kind, s.last_run_started_at, s.last_run_finished_at, \
                s.last_success_at, s.last_error_at, s.last_error, s.consecutive_failures, \
                COALESCE(SUM(CASE WHEN t.status = 'queued' THEN 1 ELSE 0 END), 0) AS queued_tasks, \
                COALESCE(SUM(CASE WHEN t.status = 'running' THEN 1 ELSE 0 END), 0) AS running_tasks, \
                COALESCE(SUM(CASE WHEN t.status = 'failed' THEN 1 ELSE 0 END), 0) AS failed_tasks \
         FROM collection_schedule_state s \
         LEFT JOIN tasks t \
           ON t.source_id = s.source_id \
          AND t.collection_id = s.collection_id \
          AND t.status IN ('queued', 'running', 'failed') \
         GROUP BY s.source_id, s.collection_id, s.scheduler_enabled, s.status, s.next_task_kind, \
                  s.next_run_at, s.active_task_id, s.active_task_kind, s.last_task_id, \
                  s.last_task_kind, s.last_run_started_at, s.last_run_finished_at, \
                  s.last_success_at, s.last_error_at, s.last_error, s.consecutive_failures \
         ORDER BY s.source_id, s.collection_id",
    ) {
        Ok(stmt) => stmt,
        Err(_) => return Vec::new(),
    };

    let rows = match stmt.query_map([], |row| {
        Ok(CollectionScheduleSnapshot {
            source_id: row.get(0)?,
            collection_id: row.get(1)?,
            scheduler_enabled: row.get::<_, i64>(2)? != 0,
            status: row.get(3)?,
            next_task_kind: row.get(4)?,
            next_run_at: row.get(5)?,
            active_task_id: row.get(6)?,
            active_task_kind: row.get(7)?,
            last_task_id: row.get(8)?,
            last_task_kind: row.get(9)?,
            last_run_started_at: row.get(10)?,
            last_run_finished_at: row.get(11)?,
            last_success_at: row.get(12)?,
            last_error_at: row.get(13)?,
            last_error: row.get(14)?,
            consecutive_failures: row.get(15)?,
            queued_tasks: row.get(16)?,
            running_tasks: row.get(17)?,
            failed_tasks: row.get(18)?,
        })
    }) {
        Ok(rows) => rows,
        Err(_) => return Vec::new(),
    };

    rows.filter_map(Result::ok).collect()
}
