use std::fs;
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use ingest_core::{DiscoveredArtifact, LocalArtifact};
use sha2::{Digest, Sha256};

pub async fn fetch_archive(
    client: &reqwest::Client,
    artifact: &DiscoveredArtifact,
    output_dir: &Path,
) -> Result<LocalArtifact> {
    let archive_name = artifact
        .metadata
        .acquisition_uri
        .rsplit('/')
        .next()
        .ok_or_else(|| anyhow!("missing archive name in URI"))?;
    let archive_path = output_dir.join(archive_name);

    if !archive_path.exists() {
        let response = client
            .get(&artifact.metadata.acquisition_uri)
            .send()
            .await
            .with_context(|| format!("downloading {}", artifact.metadata.acquisition_uri))?
            .error_for_status()
            .with_context(|| {
                format!("download failed for {}", artifact.metadata.acquisition_uri)
            })?;
        let bytes = response.bytes().await?;
        fs::write(&archive_path, &bytes)?;
    }

    let bytes = fs::read(&archive_path)?;
    let mut metadata = artifact.metadata.clone();
    metadata.fetched_at = Some(Utc::now());
    metadata.content_sha256 = Some(format!("{:x}", Sha256::digest(&bytes)));
    metadata.content_length_bytes = Some(bytes.len() as u64);

    Ok(LocalArtifact {
        metadata,
        local_path: archive_path,
    })
}
