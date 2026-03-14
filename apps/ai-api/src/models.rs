use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthUser {
    pub id: String,
    pub email: String,
    pub name: String,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub profile_picture_url: Option<String>,
    pub org_id: String,
    pub role: String,
    pub permissions: Vec<String>,
    pub session_id: String,
    pub is_admin: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticObject {
    pub source_id: String,
    pub object_name: String,
    pub object_kind: String,
    pub description: String,
    pub grain: String,
    pub time_column: String,
    pub dimensions: Vec<String>,
    pub measures: Vec<String>,
    pub join_keys: Vec<String>,
    pub caveats: Vec<String>,
    pub question_tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub status: String,
    pub sql: String,
    pub used_objects: Vec<String>,
    pub data_description: String,
    pub note: String,
    pub chart_title: String,
    #[serde(default)]
    pub chart_type: String,
    #[serde(default)]
    pub x: Option<String>,
    #[serde(default)]
    pub y: Vec<String>,
    #[serde(default)]
    pub y2: Vec<String>,
    #[serde(default)]
    pub color: Option<String>,
    #[serde(default)]
    pub y_label: Option<String>,
    #[serde(default)]
    pub y2_label: Option<String>,
    pub confidence: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "renderer", rename_all = "snake_case")]
pub enum ChartSpec {
    Summary { title: String },
    Table { title: String },
    Plotly { title: String, figure: Value },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPreview {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub row_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalAnswer {
    pub answer: String,
    pub note: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChatRequest {
    pub question: String,
    pub conversation_id: Option<Uuid>,
    pub approved_proposal: Option<String>,
    #[serde(default)]
    pub thread_context: Vec<ThreadContextMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadContextMessage {
    pub role: String,
    pub content: String,
    #[serde(default)]
    pub sql_text: Option<String>,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct MeResponse {
    pub user: AuthUser,
    pub subscription_status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConversationSummary {
    pub id: Uuid,
    pub title: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MessageRecord {
    pub id: Uuid,
    pub run_id: Option<Uuid>,
    pub role: String,
    pub content: String,
    pub sql_text: Option<String>,
    pub metadata: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConversationDetail {
    pub conversation: ConversationSummary,
    pub messages: Vec<MessageRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UsageSnapshot {
    pub llm_requests: i64,
    pub clickhouse_queries: i64,
    pub estimated_cost_usd: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamPayload {
    RunStarted {
        run_id: Uuid,
        conversation_id: Uuid,
    },
    Status {
        phase: String,
        message: String,
    },
    Plan {
        plan: Plan,
    },
    Sql {
        sql: String,
    },
    QueryPreview {
        preview: QueryPreview,
        chart: ChartSpec,
    },
    Answer {
        answer: FinalAnswer,
    },
    Completed {
        run_id: Uuid,
    },
    Failed {
        message: String,
    },
}
