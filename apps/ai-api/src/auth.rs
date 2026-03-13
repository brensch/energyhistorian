use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow};
use axum::{
    extract::{FromRef, FromRequestParts},
    http::{HeaderValue, header, request::Parts},
};
use chrono::{Duration as ChronoDuration, Utc};
use jsonwebtoken::{Algorithm, DecodingKey, TokenData, Validation, decode, decode_header};
use reqwest::Client;
use serde::Deserialize;
use tokio::sync::RwLock;

use crate::{error::AppError, models::AuthUser, state::AppState};

#[derive(Clone)]
pub struct WorkosAuth {
    issuer: String,
    audience: String,
    admin_emails: Arc<Vec<String>>,
    client: Client,
    jwks: Arc<RwLock<JwksCache>>,
}

#[derive(Default)]
struct JwksCache {
    keys: HashMap<String, DecodingKey>,
    expires_at: Option<chrono::DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
struct OpenIdConfig {
    jwks_uri: String,
}

#[derive(Debug, Deserialize)]
struct JwksResponse {
    keys: Vec<Jwk>,
}

#[derive(Debug, Deserialize)]
struct Jwk {
    kid: String,
    kty: String,
    n: Option<String>,
    e: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Claims {
    sub: String,
    email: String,
    name: Option<String>,
    org_id: Option<String>,
    role: Option<String>,
    permissions: Option<Vec<String>>,
    sid: Option<String>,
}

#[derive(Clone)]
pub struct AuthenticatedUser(pub AuthUser);

impl WorkosAuth {
    pub fn new(issuer: String, audience: String, admin_emails: Vec<String>) -> Result<Self> {
        Ok(Self {
            issuer,
            audience,
            admin_emails: Arc::new(admin_emails),
            client: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .context("building WorkOS auth client")?,
            jwks: Arc::new(RwLock::new(JwksCache::default())),
        })
    }

    async fn verify_bearer(&self, token: &str) -> Result<AuthUser> {
        let header = decode_header(token)?;
        let kid = header.kid.context("JWT kid missing")?;
        let keys = self.keys().await?;
        let key = keys.get(&kid).context("no decoding key for kid")?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_audience(&[self.audience.clone()]);
        validation.set_issuer(&[self.issuer.clone()]);
        validation.validate_exp = true;
        let token: TokenData<Claims> = decode(token, key, &validation)?;
        let claims = token.claims;
        let email = claims.email.to_lowercase();
        Ok(AuthUser {
            id: claims.sub,
            email: claims.email,
            name: claims.name.unwrap_or_else(|| email.clone()),
            org_id: claims.org_id.unwrap_or_default(),
            role: claims.role.unwrap_or_default(),
            permissions: claims.permissions.unwrap_or_default(),
            session_id: claims.sid.unwrap_or_default(),
            is_admin: self.admin_emails.contains(&email),
        })
    }

    async fn keys(&self) -> Result<HashMap<String, DecodingKey>> {
        {
            let cache = self.jwks.read().await;
            if cache
                .expires_at
                .map(|expires_at| expires_at > Utc::now())
                .unwrap_or(false)
                && !cache.keys.is_empty()
            {
                return Ok(cache.keys.clone());
            }
        }

        let openid = self
            .client
            .get(format!(
                "{}/.well-known/openid-configuration",
                self.issuer.trim_end_matches('/')
            ))
            .send()
            .await?
            .error_for_status()?
            .json::<OpenIdConfig>()
            .await?;
        let jwks = self
            .client
            .get(openid.jwks_uri)
            .send()
            .await?
            .error_for_status()?
            .json::<JwksResponse>()
            .await?;

        let mut keys = HashMap::new();
        for jwk in jwks.keys {
            if jwk.kty != "RSA" {
                continue;
            }
            if let (Some(n), Some(e)) = (jwk.n.as_deref(), jwk.e.as_deref()) {
                keys.insert(jwk.kid, DecodingKey::from_rsa_components(n, e)?);
            }
        }
        if keys.is_empty() {
            return Err(anyhow!("no RSA keys available from WorkOS JWKS"));
        }

        let mut cache = self.jwks.write().await;
        cache.keys = keys.clone();
        cache.expires_at = Some(Utc::now() + ChronoDuration::minutes(30));
        Ok(keys)
    }

    fn dev_user(parts: &Parts) -> AuthUser {
        let header_value = |name: &str, fallback: &str| {
            parts
                .headers
                .get(name)
                .and_then(|value: &HeaderValue| value.to_str().ok())
                .filter(|value: &&str| !value.is_empty())
                .unwrap_or(fallback)
                .to_string()
        };
        AuthUser {
            id: header_value("X-Dev-User-Id", "dev-user"),
            email: header_value("X-Dev-User-Email", "dev@example.com"),
            name: header_value("X-Dev-User-Name", "Developer"),
            org_id: header_value("X-Dev-Org-Id", "dev-org"),
            role: header_value("X-Dev-Role", "admin"),
            permissions: Vec::new(),
            session_id: "dev-session".to_string(),
            is_admin: true,
        }
    }
}

impl<S> FromRequestParts<S> for AuthenticatedUser
where
    AppState: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let app_state = AppState::from_ref(state);
        let auth_header = parts.headers.get(header::AUTHORIZATION);
        if auth_header.is_none() && app_state.config.allow_dev_auth {
            return Ok(Self(WorkosAuth::dev_user(parts)));
        }

        let token = auth_header
            .and_then(|value: &HeaderValue| value.to_str().ok())
            .and_then(|value: &str| value.strip_prefix("Bearer "))
            .ok_or(AppError::Unauthorized)?;
        let user = app_state
            .auth
            .verify_bearer(token)
            .await
            .map_err(|_| AppError::Unauthorized)?;
        if user.org_id.is_empty() {
            return Err(AppError::Internal(anyhow!("user org_id missing")));
        }
        Ok(Self(user))
    }
}
