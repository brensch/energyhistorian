use anyhow::{Context, Result, anyhow, bail};
use chrono::{TimeZone, Utc};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use sha2::Sha256;

use crate::{config::Config, db::Store, models::AuthUser};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct StripeClient {
    http: Client,
    secret_key: String,
    webhook_secret: String,
    price_id: String,
    success_url: String,
    cancel_url: String,
    portal_return_url: String,
}

impl StripeClient {
    pub fn from_config(config: &Config) -> Option<Self> {
        Some(Self {
            http: Client::new(),
            secret_key: config.stripe_secret_key.clone()?,
            webhook_secret: config.stripe_webhook_secret.clone()?,
            price_id: config.stripe_price_id.clone()?,
            success_url: config
                .stripe_success_url
                .clone()
                .unwrap_or_else(|| config.frontend_url.clone()),
            cancel_url: config
                .stripe_cancel_url
                .clone()
                .unwrap_or_else(|| config.frontend_url.clone()),
            portal_return_url: config
                .stripe_portal_return_url
                .clone()
                .unwrap_or_else(|| config.frontend_url.clone()),
        })
    }

    pub async fn create_checkout_session(&self, user: &AuthUser) -> Result<String> {
        let form = [
            ("mode", "subscription".to_string()),
            ("success_url", self.success_url.clone()),
            ("cancel_url", self.cancel_url.clone()),
            ("client_reference_id", user.org_id.clone()),
            ("customer_email", user.email.clone()),
            ("line_items[0][price]", self.price_id.clone()),
            ("line_items[0][quantity]", "1".to_string()),
            ("subscription_data[metadata][org_id]", user.org_id.clone()),
            ("subscription_data[metadata][user_id]", user.id.clone()),
        ];
        let response = self
            .http
            .post("https://api.stripe.com/v1/checkout/sessions")
            .bearer_auth(&self.secret_key)
            .form(&form)
            .send()
            .await?;
        if !response.status().is_success() {
            bail!(
                "stripe error {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            );
        }
        let body = response.json::<CheckoutSession>().await?;
        Ok(body.url)
    }

    pub async fn create_portal_session(&self, customer_id: &str) -> Result<String> {
        let form = [
            ("customer", customer_id.to_string()),
            ("return_url", self.portal_return_url.clone()),
        ];
        let response = self
            .http
            .post("https://api.stripe.com/v1/billing_portal/sessions")
            .bearer_auth(&self.secret_key)
            .form(&form)
            .send()
            .await?;
        if !response.status().is_success() {
            bail!(
                "stripe error {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            );
        }
        let body = response.json::<PortalSession>().await?;
        Ok(body.url)
    }

    pub fn verify_webhook(&self, raw_body: &str, signature: &str) -> Result<StripeEvent> {
        let (timestamp, signatures) = parse_signature_header(signature)?;
        let age = Utc::now().timestamp() - timestamp;
        if !(0..=300).contains(&age) {
            bail!("stale stripe signature timestamp");
        }
        let signed_payload = format!("{timestamp}.{raw_body}");
        let mut mac = HmacSha256::new_from_slice(self.webhook_secret.as_bytes())?;
        mac.update(signed_payload.as_bytes());
        let expected = hex::encode(mac.finalize().into_bytes());
        if !signatures.iter().any(|candidate| candidate == &expected) {
            bail!("invalid stripe signature");
        }
        Ok(serde_json::from_str(raw_body)?)
    }
}

pub async fn apply_webhook(store: &Store, event: &StripeEvent) -> Result<()> {
    store
        .save_webhook_event(
            &event.id,
            "stripe",
            &event.event_type,
            serde_json::to_value(event)?,
        )
        .await?;
    match event.event_type.as_str() {
        "customer.subscription.created"
        | "customer.subscription.updated"
        | "customer.subscription.deleted" => {
            let object = event
                .data
                .object
                .as_object()
                .context("missing stripe object")?;
            let metadata = object
                .get("metadata")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            let org_id = metadata
                .get("org_id")
                .and_then(Value::as_str)
                .map(ToString::to_string)
                .or_else(|| {
                    object
                        .get("client_reference_id")
                        .and_then(Value::as_str)
                        .map(ToString::to_string)
                })
                .ok_or_else(|| anyhow!("stripe webhook missing org_id metadata"))?;
            let status = object
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("incomplete");
            let customer_id = object.get("customer").and_then(Value::as_str);
            let subscription_id = object.get("id").and_then(Value::as_str);
            let price_id = event
                .data
                .object
                .pointer("/items/data/0/price/id")
                .and_then(Value::as_str);
            let current_period_end = object
                .get("current_period_end")
                .and_then(Value::as_i64)
                .and_then(|value| Utc.timestamp_opt(value, 0).single());
            store
                .upsert_subscription(
                    &org_id,
                    customer_id,
                    subscription_id,
                    status,
                    price_id,
                    current_period_end,
                )
                .await?;
        }
        _ => {}
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct CheckoutSession {
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct PortalSession {
    pub url: String,
}

#[derive(Debug, Deserialize, serde::Serialize)]
pub struct StripeEvent {
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: StripeEventData,
}

#[derive(Debug, Deserialize, serde::Serialize)]
pub struct StripeEventData {
    pub object: Value,
}

fn parse_signature_header(header: &str) -> Result<(i64, Vec<String>)> {
    let mut timestamp = None;
    let mut signatures = Vec::new();
    for part in header.split(',') {
        let mut pieces = part.splitn(2, '=');
        let key = pieces.next().unwrap_or_default().trim();
        let value = pieces.next().unwrap_or_default().trim();
        match key {
            "t" => timestamp = value.parse::<i64>().ok(),
            "v1" => signatures.push(value.to_string()),
            _ => {}
        }
    }
    let timestamp = timestamp.ok_or_else(|| anyhow!("stripe signature missing timestamp"))?;
    Ok((timestamp, signatures))
}
