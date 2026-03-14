use anyhow::{Result, anyhow, bail};
use reqwest::Client;
use serde::Deserialize;

use crate::config::{Config, LlmProvider};

#[derive(Clone)]
pub struct LlmClient {
    provider: LlmProvider,
    http: Client,
    openai_api_key: Option<String>,
    openai_base_url: String,
    openai_model: String,
    anthropic_api_key: Option<String>,
    anthropic_base_url: String,
    anthropic_model: String,
}

#[derive(Debug, Clone)]
pub struct CompletionResponse {
    pub text: String,
    pub model: String,
    pub input_tokens: i64,
    pub output_tokens: i64,
}

impl LlmClient {
    pub fn new(config: &Config) -> Result<Self> {
        Ok(Self {
            provider: config.llm_provider,
            http: Client::builder().build()?,
            openai_api_key: config.openai_api_key.clone(),
            openai_base_url: config.openai_base_url.clone(),
            openai_model: config.openai_model.clone(),
            anthropic_api_key: config.anthropic_api_key.clone(),
            anthropic_base_url: config.anthropic_base_url.clone(),
            anthropic_model: config.anthropic_model.clone(),
        })
    }

    pub async fn json_completion(&self, system: &str, user: &str) -> Result<CompletionResponse> {
        self.json_completion_with_max_tokens(system, user, 2048).await
    }

    pub async fn json_completion_with_max_tokens(
        &self,
        system: &str,
        user: &str,
        max_tokens: u32,
    ) -> Result<CompletionResponse> {
        match self.provider {
            LlmProvider::OpenAi => self.openai_completion(system, user, max_tokens).await,
            LlmProvider::Anthropic => self.anthropic_completion(system, user, max_tokens).await,
        }
    }

    async fn openai_completion(
        &self,
        system: &str,
        user: &str,
        max_tokens: u32,
    ) -> Result<CompletionResponse> {
        let api_key = self
            .openai_api_key
            .as_deref()
            .ok_or_else(|| anyhow!("OPENAI_API_KEY missing"))?;
        let payload = serde_json::json!({
            "model": self.openai_model,
            "max_output_tokens": max_tokens,
            "input": [
                { "role": "system", "content": [{ "type": "input_text", "text": system }] },
                { "role": "user", "content": [{ "type": "input_text", "text": user }] }
            ]
        });
        let response = self
            .http
            .post(format!(
                "{}/responses",
                self.openai_base_url.trim_end_matches('/')
            ))
            .bearer_auth(api_key)
            .json(&payload)
            .send()
            .await?;
        if !response.status().is_success() {
            bail!(
                "openai error {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            );
        }
        let payload = response.json::<OpenAiResponse>().await?;
        let mut text = payload.output_text.unwrap_or_default();
        if text.is_empty() {
            let mut parts = Vec::new();
            for item in payload.output {
                for content in item.content {
                    if let Some(chunk) = content.text {
                        parts.push(chunk);
                    }
                }
            }
            text = parts.join("\n");
        }
        Ok(CompletionResponse {
            text,
            model: payload.model,
            input_tokens: payload
                .usage
                .as_ref()
                .map(|usage| usage.input_tokens)
                .unwrap_or(0),
            output_tokens: payload.output_tokens.unwrap_or_else(|| {
                payload
                    .usage
                    .as_ref()
                    .map(|usage| usage.output_tokens)
                    .unwrap_or(0)
            }),
        })
    }

    async fn anthropic_completion(
        &self,
        system: &str,
        user: &str,
        max_tokens: u32,
    ) -> Result<CompletionResponse> {
        let api_key = self
            .anthropic_api_key
            .as_deref()
            .ok_or_else(|| anyhow!("ANTHROPIC_API_KEY missing"))?;
        let payload = serde_json::json!({
            "model": self.anthropic_model,
            "max_tokens": max_tokens,
            "system": system,
            "messages": [
                {
                    "role": "user",
                    "content": user
                }
            ]
        });
        let response = self
            .http
            .post(format!(
                "{}/messages",
                self.anthropic_base_url.trim_end_matches('/')
            ))
            .header("x-api-key", api_key)
            .header("anthropic-version", "2023-06-01")
            .json(&payload)
            .send()
            .await?;
        if !response.status().is_success() {
            bail!(
                "anthropic error {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            );
        }
        let payload = response.json::<AnthropicResponse>().await?;
        let text = payload
            .content
            .into_iter()
            .filter(|block| block.kind == "text")
            .filter_map(|block| block.text)
            .collect::<Vec<_>>()
            .join("\n");
        Ok(CompletionResponse {
            text,
            model: payload.model,
            input_tokens: payload.usage.input_tokens,
            output_tokens: payload.usage.output_tokens,
        })
    }
}

pub fn extract_json<T>(text: &str) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let trimmed = text.trim();
    if let Ok(parsed) = serde_json::from_str(trimmed) {
        return Ok(parsed);
    }
    if let Some(start) = trimmed.find('{') {
        if let Some(end) = trimmed.rfind('}') {
            let candidate = &trimmed[start..=end];
            if let Ok(parsed) = serde_json::from_str(candidate) {
                return Ok(parsed);
            }
        }
    }
    Err(anyhow!("model response did not contain valid JSON"))
}

pub fn estimate_cost_usd(
    provider: LlmProvider,
    model: &str,
    input_tokens: i64,
    output_tokens: i64,
) -> f64 {
    match provider {
        LlmProvider::OpenAi => {
            if model.contains("gpt-5") {
                (input_tokens as f64 * 0.00000125) + (output_tokens as f64 * 0.00001)
            } else {
                0.0
            }
        }
        LlmProvider::Anthropic => {
            if model.contains("haiku") {
                (input_tokens as f64 * 0.0000008) + (output_tokens as f64 * 0.000004)
            } else if model.contains("sonnet") {
                (input_tokens as f64 * 0.000003) + (output_tokens as f64 * 0.000015)
            } else {
                0.0
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct OpenAiResponse {
    model: String,
    #[serde(default)]
    output_text: Option<String>,
    #[serde(default)]
    output_tokens: Option<i64>,
    #[serde(default)]
    usage: Option<OpenAiUsage>,
    #[serde(default)]
    output: Vec<OpenAiOutputItem>,
}

#[derive(Debug, Deserialize)]
struct OpenAiUsage {
    #[serde(default)]
    input_tokens: i64,
    #[serde(default)]
    output_tokens: i64,
}

#[derive(Debug, Deserialize)]
struct OpenAiOutputItem {
    #[serde(default)]
    content: Vec<OpenAiContent>,
}

#[derive(Debug, Deserialize)]
struct OpenAiContent {
    #[serde(default)]
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AnthropicResponse {
    model: String,
    content: Vec<AnthropicContent>,
    usage: AnthropicUsage,
}

#[derive(Debug, Deserialize)]
struct AnthropicContent {
    #[serde(rename = "type")]
    kind: String,
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AnthropicUsage {
    input_tokens: i64,
    output_tokens: i64,
}
