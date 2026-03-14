use std::str::FromStr;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use serde_json::{Value, json};
use tokio_postgres::{NoTls, Row};
use uuid::Uuid;

use crate::models::{
    AuthUser, ChartSpec, ConversationDetail, ConversationSummary, MessageRecord, QueryPreview,
    UsageSnapshot,
};

#[derive(Clone)]
pub struct Store {
    pool: Pool,
}

#[derive(Debug, Clone)]
pub struct SubscriptionRecord {
    pub stripe_customer_id: Option<String>,
}

impl Store {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let config = tokio_postgres::Config::from_str(database_url)
            .with_context(|| format!("parsing DATABASE_URL {database_url}"))?;
        let manager = Manager::from_config(
            config,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(manager).max_size(16).build()?;
        Ok(Self { pool })
    }

    pub async fn migrate(&self) -> Result<()> {
        let client = self.pool.get().await?;
        let sql = include_str!("../migrations/0001_init.sql");
        for statement in sql
            .split(';')
            .map(str::trim)
            .filter(|statement| !statement.is_empty())
        {
            client.execute(statement, &[]).await?;
        }
        Ok(())
    }

    pub async fn sync_user(&self, user: &AuthUser) -> Result<()> {
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        tx.execute(
            "INSERT INTO organizations (id, name) VALUES ($1, $2)
             ON CONFLICT (id) DO UPDATE SET updated_at = now()",
            &[&user.org_id, &user.org_id],
        )
        .await?;
        tx.execute(
            "INSERT INTO users (id, email, name, first_name, last_name, profile_picture_url)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (id) DO UPDATE
             SET email = EXCLUDED.email,
                 name = EXCLUDED.name,
                 first_name = EXCLUDED.first_name,
                 last_name = EXCLUDED.last_name,
                 profile_picture_url = EXCLUDED.profile_picture_url,
                 updated_at = now()",
            &[
                &user.id,
                &user.email,
                &user.name,
                &user.first_name,
                &user.last_name,
                &user.profile_picture_url,
            ],
        )
        .await?;
        tx.execute(
            "INSERT INTO memberships (org_id, user_id, role, is_admin) VALUES ($1, $2, $3, $4)
             ON CONFLICT (org_id, user_id) DO UPDATE
             SET role = EXCLUDED.role, is_admin = EXCLUDED.is_admin, updated_at = now()",
            &[&user.org_id, &user.id, &user.role, &user.is_admin],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn subscription_status(&self, org_id: &str) -> Result<String> {
        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                "SELECT status FROM subscriptions WHERE org_id = $1",
                &[&org_id],
            )
            .await?;
        Ok(row
            .map(|row| row.get::<_, String>(0))
            .unwrap_or_else(|| "inactive".to_string()))
    }

    pub async fn latest_subscription(&self, org_id: &str) -> Result<Option<SubscriptionRecord>> {
        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                "SELECT stripe_customer_id FROM subscriptions WHERE org_id = $1",
                &[&org_id],
            )
            .await?;
        Ok(row.map(|row| SubscriptionRecord {
            stripe_customer_id: row.get(0),
        }))
    }

    pub async fn create_conversation(
        &self,
        org_id: &str,
        user_id: &str,
        title: &str,
    ) -> Result<ConversationSummary> {
        let client = self.pool.get().await?;
        let row = client
            .query_one(
                "INSERT INTO conversations (id, org_id, created_by, title)
                 VALUES ($1, $2, $3, $4)
                 RETURNING id, title, created_at, updated_at",
                &[&Uuid::new_v4(), &org_id, &user_id, &title],
            )
            .await?;
        Ok(map_conversation_summary(&row))
    }

    pub async fn ensure_conversation(
        &self,
        org_id: &str,
        user_id: &str,
        conversation_id: Option<Uuid>,
        title_hint: &str,
    ) -> Result<ConversationSummary> {
        if let Some(id) = conversation_id {
            let client = self.pool.get().await?;
            if let Some(row) = client
                .query_opt(
                    "SELECT id, title, created_at, updated_at
                     FROM conversations
                     WHERE id = $1 AND org_id = $2 AND created_by = $3",
                    &[&id, &org_id, &user_id],
                )
                .await?
            {
                return Ok(map_conversation_summary(&row));
            }
        }
        self.create_conversation(org_id, user_id, title_hint).await
    }

    pub async fn list_conversations(
        &self,
        org_id: &str,
        user_id: &str,
    ) -> Result<Vec<ConversationSummary>> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT id, title, created_at, updated_at
                 FROM conversations
                 WHERE org_id = $1 AND created_by = $2
                 ORDER BY updated_at DESC",
                &[&org_id, &user_id],
            )
            .await?;
        Ok(rows.iter().map(map_conversation_summary).collect())
    }

    pub async fn get_conversation(
        &self,
        org_id: &str,
        user_id: &str,
        id: Uuid,
    ) -> Result<Option<ConversationDetail>> {
        let client = self.pool.get().await?;
        let Some(conversation_row) = client
            .query_opt(
                "SELECT id, title, created_at, updated_at
                 FROM conversations
                 WHERE id = $1 AND org_id = $2 AND created_by = $3",
                &[&id, &org_id, &user_id],
            )
            .await?
        else {
            return Ok(None);
        };

        let rows = client
            .query(
                "SELECT id, run_id, role, content, sql_text, metadata, created_at
                 FROM messages
                 WHERE conversation_id = $1
                 ORDER BY created_at ASC",
                &[&id],
            )
            .await?;

        Ok(Some(ConversationDetail {
            conversation: map_conversation_summary(&conversation_row),
            messages: rows.iter().map(map_message_record).collect(),
        }))
    }

    pub async fn create_run(
        &self,
        conversation_id: Uuid,
        org_id: &str,
        user_id: &str,
        question: &str,
    ) -> Result<Uuid> {
        let client = self.pool.get().await?;
        let run_id = Uuid::new_v4();
        client
            .execute(
                "INSERT INTO chat_runs (id, conversation_id, org_id, user_id, question, status)
                 VALUES ($1, $2, $3, $4, $5, 'running')",
                &[&run_id, &conversation_id, &org_id, &user_id, &question],
            )
            .await?;
        Ok(run_id)
    }

    pub async fn finish_run(
        &self,
        run_id: Uuid,
        status: &str,
        sql_text: Option<&str>,
        error_text: Option<&str>,
    ) -> Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                "UPDATE chat_runs
                 SET status = $2, sql_text = COALESCE($3, sql_text), error_text = $4, completed_at = now()
                 WHERE id = $1",
                &[&run_id, &status, &sql_text, &error_text],
            )
            .await?;
        Ok(())
    }

    pub async fn append_message(
        &self,
        conversation_id: Uuid,
        org_id: &str,
        user_id: &str,
        run_id: Option<Uuid>,
        role: &str,
        content: &str,
        sql_text: Option<&str>,
        metadata: Value,
    ) -> Result<()> {
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        tx.execute(
            "INSERT INTO messages (id, conversation_id, org_id, user_id, run_id, role, content, sql_text, metadata)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            &[&Uuid::new_v4(), &conversation_id, &org_id, &user_id, &run_id, &role, &content, &sql_text, &metadata],
        )
        .await?;
        tx.execute(
            "UPDATE conversations SET updated_at = now() WHERE id = $1",
            &[&conversation_id],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn record_usage(
        &self,
        org_id: &str,
        user_id: &str,
        run_id: Option<Uuid>,
        kind: &str,
        quantity: i64,
        unit: &str,
        metadata: Value,
    ) -> Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                "INSERT INTO usage_events (id, org_id, user_id, run_id, kind, quantity, unit, metadata)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                &[&Uuid::new_v4(), &org_id, &user_id, &run_id, &kind, &quantity, &unit, &metadata],
            )
            .await?;
        Ok(())
    }

    pub async fn usage_snapshot(&self, org_id: &str, user_id: &str) -> Result<UsageSnapshot> {
        let client = self.pool.get().await?;
        let row = client
            .query_one(
                "SELECT
                    COALESCE(SUM(CASE WHEN kind = 'llm_request' THEN quantity ELSE 0 END), 0)::bigint,
                    COALESCE(SUM(CASE WHEN kind = 'clickhouse_query' THEN quantity ELSE 0 END), 0)::bigint,
                    COALESCE(SUM(CASE WHEN kind = 'llm_cost_usd_micros' THEN quantity ELSE 0 END), 0)::bigint
                 FROM usage_events
                 WHERE org_id = $1 AND user_id = $2 AND created_at >= date_trunc('month', now())",
                &[&org_id, &user_id],
            )
            .await?;
        Ok(UsageSnapshot {
            llm_requests: row.get::<_, i64>(0),
            clickhouse_queries: row.get::<_, i64>(1),
            estimated_cost_usd: row.get::<_, i64>(2) as f64 / 1_000_000.0,
        })
    }

    pub async fn save_webhook_event(
        &self,
        id: &str,
        source: &str,
        event_type: &str,
        payload: Value,
    ) -> Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                "INSERT INTO webhook_events (id, source, event_type, payload)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (id) DO NOTHING",
                &[&id, &source, &event_type, &payload],
            )
            .await?;
        Ok(())
    }

    pub async fn upsert_subscription(
        &self,
        org_id: &str,
        stripe_customer_id: Option<&str>,
        stripe_subscription_id: Option<&str>,
        status: &str,
        price_id: Option<&str>,
        current_period_end: Option<DateTime<Utc>>,
    ) -> Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                "INSERT INTO subscriptions (org_id, stripe_customer_id, stripe_subscription_id, status, price_id, current_period_end)
                 VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT (org_id) DO UPDATE
                 SET stripe_customer_id = EXCLUDED.stripe_customer_id,
                     stripe_subscription_id = EXCLUDED.stripe_subscription_id,
                     status = EXCLUDED.status,
                     price_id = EXCLUDED.price_id,
                     current_period_end = EXCLUDED.current_period_end,
                     updated_at = now()",
                &[&org_id, &stripe_customer_id, &stripe_subscription_id, &status, &price_id, &current_period_end],
            )
            .await?;
        Ok(())
    }

    pub fn assistant_metadata(
        sql: Option<&str>,
        used_objects: &[String],
        chart: Option<&ChartSpec>,
        preview: Option<&QueryPreview>,
        note: Option<&str>,
        confidence: Option<&str>,
    ) -> Value {
        json!({
            "sql": sql,
            "used_objects": used_objects,
            "chart": chart,
            "preview": preview,
            "note": note,
            "confidence": confidence
        })
    }
}

fn map_conversation_summary(row: &Row) -> ConversationSummary {
    ConversationSummary {
        id: row.get("id"),
        title: row.get("title"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn map_message_record(row: &Row) -> MessageRecord {
    MessageRecord {
        id: row.get("id"),
        run_id: row.get("run_id"),
        role: row.get("role"),
        content: row.get("content"),
        sql_text: row.get("sql_text"),
        metadata: row.get("metadata"),
        created_at: row.get("created_at"),
    }
}
