use std::fs;
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Builder as S3ConfigBuilder, Region};
use aws_sdk_s3::primitives::ByteStream;
use controlplane::ObjectStoreConfig;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipArchive, ZipWriter};

#[derive(Debug, Clone)]
pub struct StoredObject {
    pub bucket: String,
    pub key: String,
    pub content_type: String,
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

    pub async fn put_path(&self, key: &str, path: &Path) -> Result<StoredObject> {
        let (bytes, content_type) = if path.is_dir() {
            let path = path.to_path_buf();
            tokio::task::spawn_blocking(move || archive_directory(&path))
                .await
                .context("joining directory archive task")??
        } else {
            let bytes = tokio::fs::read(path)
                .await
                .with_context(|| format!("reading file {}", path.display()))?;
            (bytes, "application/octet-stream".to_string())
        };
        let sha256 = format!("{:x}", Sha256::digest(&bytes));
        let content_length_bytes = bytes.len() as u64;
        let body = ByteStream::from(bytes);
        let response = self
            .client
            .put_object()
            .bucket(&self.config.bucket)
            .key(key)
            .content_type(content_type.clone())
            .body(body)
            .send()
            .await
            .with_context(|| format!("uploading s3://{}/{key}", self.config.bucket))?;

        Ok(StoredObject {
            bucket: self.config.bucket.clone(),
            key: key.to_string(),
            content_type,
            sha256,
            content_length_bytes,
            etag: response.e_tag,
        })
    }

    pub async fn fetch_to_path(
        &self,
        key: &str,
        path: &Path,
        content_type: &str,
    ) -> Result<PathBuf> {
        let response = self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("downloading s3://{}/{key}", self.config.bucket))?;
        let data = response.body.collect().await?.into_bytes();
        if content_type == "application/x-energyhistorian-directory-zip" {
            let target_dir = path.to_path_buf();
            tokio::task::spawn_blocking(move || extract_directory_archive(&data, &target_dir))
                .await
                .context("joining directory extract task")??;
            return Ok(path.to_path_buf());
        }
        let mut file = tokio::fs::File::create(path)
            .await
            .with_context(|| format!("creating {}", path.display()))?;
        file.write_all(&data)
            .await
            .with_context(|| format!("writing {}", path.display()))?;
        file.flush().await?;
        Ok(path.to_path_buf())
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

fn archive_directory(path: &Path) -> Result<(Vec<u8>, String)> {
    let cursor = Cursor::new(Vec::new());
    let mut zip = ZipWriter::new(cursor);
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
    let mut stack = vec![path.to_path_buf()];

    while let Some(current) = stack.pop() {
        for entry in fs::read_dir(&current)
            .with_context(|| format!("reading directory {}", current.display()))?
        {
            let entry = entry?;
            let entry_path = entry.path();
            if entry_path.is_dir() {
                stack.push(entry_path);
                continue;
            }
            let relative = entry_path
                .strip_prefix(path)
                .with_context(|| format!("stripping archive prefix {}", path.display()))?;
            let relative_name = relative.to_string_lossy().replace('\\', "/");
            zip.start_file(&relative_name, options)
                .with_context(|| format!("starting archive entry {relative_name}"))?;
            let bytes = fs::read(&entry_path)
                .with_context(|| format!("reading archive entry {}", entry_path.display()))?;
            zip.write_all(&bytes)
                .with_context(|| format!("writing archive entry {relative_name}"))?;
        }
    }

    let bytes = zip.finish()?.into_inner();
    Ok((
        bytes,
        "application/x-energyhistorian-directory-zip".to_string(),
    ))
}

fn extract_directory_archive(bytes: &[u8], target_dir: &Path) -> Result<()> {
    fs::create_dir_all(target_dir)
        .with_context(|| format!("creating directory {}", target_dir.display()))?;
    let mut archive = ZipArchive::new(Cursor::new(bytes)).context("opening directory archive")?;
    for idx in 0..archive.len() {
        let mut entry = archive.by_index(idx)?;
        let enclosed = entry
            .enclosed_name()
            .map(PathBuf::from)
            .ok_or_else(|| anyhow::anyhow!("zip entry had unsafe path"))?;
        let out_path = target_dir.join(enclosed);
        if entry.is_dir() {
            fs::create_dir_all(&out_path)?;
            continue;
        }
        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut out = fs::File::create(&out_path)
            .with_context(|| format!("creating extracted file {}", out_path.display()))?;
        let mut buffer = Vec::new();
        entry.read_to_end(&mut buffer)?;
        out.write_all(&buffer)
            .with_context(|| format!("writing extracted file {}", out_path.display()))?;
    }
    Ok(())
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
