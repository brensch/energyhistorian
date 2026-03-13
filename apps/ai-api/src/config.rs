use std::env;

use anyhow::{Context, Result, bail};

#[derive(Clone, Debug)]
pub struct Config {
    pub port: u16,
    pub database_url: String,
    pub clickhouse_read_url: String,
    pub clickhouse_write_url: String,
    pub clickhouse_view_db: String,
    pub clickhouse_usage_db: String,
    pub openai_api_key: Option<String>,
    pub openai_base_url: String,
    pub openai_model: String,
    pub anthropic_api_key: Option<String>,
    pub anthropic_base_url: String,
    pub anthropic_model: String,
    pub llm_provider: LlmProvider,
    pub frontend_url: String,
    pub allow_dev_auth: bool,
    pub admin_emails: Vec<String>,
    pub workos_issuer: String,
    pub workos_audience: String,
    pub stripe_secret_key: Option<String>,
    pub stripe_webhook_secret: Option<String>,
    pub stripe_price_id: Option<String>,
    pub stripe_success_url: Option<String>,
    pub stripe_cancel_url: Option<String>,
    pub stripe_portal_return_url: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LlmProvider {
    OpenAi,
    Anthropic,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let provider = match env_or("LLM_PROVIDER", "openai")
            .to_ascii_lowercase()
            .as_str()
        {
            "openai" => LlmProvider::OpenAi,
            "anthropic" => LlmProvider::Anthropic,
            other => bail!("unsupported LLM_PROVIDER `{other}`"),
        };

        let config = Self {
            port: env_or("AI_API_PORT", &env_or("PORT", "8090"))
                .parse()
                .context("invalid AI_API_PORT")?,
            database_url: required("DATABASE_URL")?,
            clickhouse_read_url: required("CLICKHOUSE_READ_URL")?,
            clickhouse_write_url: required("CLICKHOUSE_WRITE_URL")?,
            clickhouse_view_db: env_or("CLICKHOUSE_VIEW_DB", "semantic"),
            clickhouse_usage_db: env_or("CLICKHOUSE_USAGE_DB", "tracking"),
            openai_api_key: env_var("OPENAI_API_KEY").or_else(|| env_var("openai_key")),
            openai_base_url: env_or("OPENAI_BASE_URL", "https://api.openai.com/v1"),
            openai_model: env_or("OPENAI_MODEL", "gpt-5-mini"),
            anthropic_api_key: env_var("ANTHROPIC_API_KEY"),
            anthropic_base_url: env_or("ANTHROPIC_BASE_URL", "https://api.anthropic.com/v1"),
            anthropic_model: env_or("ANTHROPIC_MODEL", "claude-sonnet-4-5"),
            llm_provider: provider,
            frontend_url: env_or("FRONTEND_URL", "http://localhost:3000"),
            allow_dev_auth: env_or("ALLOW_DEV_AUTH", "false") == "true",
            admin_emails: csv_env("ADMIN_EMAILS"),
            workos_issuer: required("WORKOS_ISSUER")?,
            workos_audience: required("WORKOS_AUDIENCE")?,
            stripe_secret_key: env_var("STRIPE_SECRET_KEY"),
            stripe_webhook_secret: env_var("STRIPE_WEBHOOK_SECRET"),
            stripe_price_id: env_var("STRIPE_PRICE_ID"),
            stripe_success_url: env_var("STRIPE_SUCCESS_URL"),
            stripe_cancel_url: env_var("STRIPE_CANCEL_URL"),
            stripe_portal_return_url: env_var("STRIPE_PORTAL_RETURN_URL"),
        };

        match config.llm_provider {
            LlmProvider::OpenAi if config.openai_api_key.is_none() => {
                bail!("OPENAI_API_KEY is required when LLM_PROVIDER=openai");
            }
            LlmProvider::Anthropic if config.anthropic_api_key.is_none() => {
                bail!("ANTHROPIC_API_KEY is required when LLM_PROVIDER=anthropic");
            }
            _ => {}
        }

        let stripe_any = config.stripe_secret_key.is_some()
            || config.stripe_webhook_secret.is_some()
            || config.stripe_price_id.is_some();
        if stripe_any
            && (config.stripe_secret_key.is_none()
                || config.stripe_webhook_secret.is_none()
                || config.stripe_price_id.is_none())
        {
            bail!(
                "stripe config is partial; set STRIPE_SECRET_KEY, STRIPE_WEBHOOK_SECRET, and STRIPE_PRICE_ID together"
            );
        }

        Ok(config)
    }
}

fn env_var(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_or(key: &str, fallback: &str) -> String {
    env_var(key).unwrap_or_else(|| fallback.to_string())
}

fn required(key: &str) -> Result<String> {
    env_var(key).with_context(|| format!("{key} is required"))
}

fn csv_env(key: &str) -> Vec<String> {
    env_var(key)
        .map(|value| {
            value
                .split(',')
                .map(|item| item.trim().to_lowercase())
                .filter(|item| !item.is_empty())
                .collect()
        })
        .unwrap_or_default()
}
