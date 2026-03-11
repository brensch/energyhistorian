use std::path::Path;

use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Builder as S3ConfigBuilder, Region};
use aws_sdk_s3::primitives::ByteStream;
use controlplane::ObjectStoreConfig;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone)]
pub struct StoredObject {
    pub bucket: String,
    pub key: String,
    pub sha256: String,
    pub content_length_bytes: u64,
    pub etag: Option<String>,
}

#[derive(Clone)]
pub struct ObjectStore {
    client: Client,
    config: ObjectStoreConfig,
}

impl ObjectStore {
    pub async fn new(config: ObjectStoreConfig) -> Result<Self> {
        let creds = Credentials::new(
            config.access_key_id.clone(),
            config.secret_access_key.clone(),
            None,
            None,
            "energyhistorian-static",
        );
        let shared = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(config.region.clone()))
            .credentials_provider(creds)
            .endpoint_url(config.endpoint.clone())
            .load()
            .await;
        let s3_config = S3ConfigBuilder::from(&shared)
            .force_path_style(config.force_path_style)
            .build();
        Ok(Self {
            client: Client::from_conf(s3_config),
            config,
        })
    }

    pub async fn put_file(&self, key: &str, path: &Path) -> Result<StoredObject> {
        let bytes = tokio::fs::read(path)
            .await
            .with_context(|| format!("reading file {}", path.display()))?;
        let sha256 = format!("{:x}", Sha256::digest(&bytes));
        let content_length_bytes = bytes.len() as u64;
        let body = ByteStream::from(bytes);
        let response = self
            .client
            .put_object()
            .bucket(&self.config.bucket)
            .key(key)
            .body(body)
            .send()
            .await
            .with_context(|| format!("uploading s3://{}/{key}", self.config.bucket))?;

        Ok(StoredObject {
            bucket: self.config.bucket.clone(),
            key: key.to_string(),
            sha256,
            content_length_bytes,
            etag: response.e_tag,
        })
    }

    pub async fn fetch_to_path(&self, key: &str, path: &Path) -> Result<()> {
        let response = self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("downloading s3://{}/{key}", self.config.bucket))?;
        let data = response.body.collect().await?.into_bytes();
        let mut file = tokio::fs::File::create(path)
            .await
            .with_context(|| format!("creating {}", path.display()))?;
        file.write_all(&data)
            .await
            .with_context(|| format!("writing {}", path.display()))?;
        file.flush().await?;
        Ok(())
    }

    pub fn artifact_key(
        &self,
        source_id: &str,
        collection_id: &str,
        artifact_id: &str,
        remote_uri: &str,
    ) -> String {
        let filename = remote_uri.rsplit('/').next().unwrap_or("artifact.bin");
        format!(
            "{}/{}/{}/{}",
            sanitize_path_component(source_id),
            sanitize_path_component(collection_id),
            sanitize_path_component(artifact_id),
            sanitize_path_component(filename)
        )
    }
}

fn sanitize_path_component(value: &str) -> String {
    let mut output = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if output.is_empty() {
        output = "artifact".to_string();
    }
    output
}
