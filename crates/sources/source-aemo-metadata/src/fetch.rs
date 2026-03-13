use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use ingest_core::{ArtifactMetadata, DiscoveredArtifact, LocalArtifact};
use reqwest::StatusCode;
use sha2::{Digest, Sha256};

use crate::discovery::sanitize_part;
use crate::mms_parser;

pub async fn fetch_artifact(
    client: &reqwest::Client,
    artifact: &DiscoveredArtifact,
    output_dir: &Path,
) -> Result<LocalArtifact> {
    fs::create_dir_all(output_dir)
        .with_context(|| format!("creating metadata output dir {}", output_dir.display()))?;

    if artifact.metadata.acquisition_uri == mms_parser::TOC_URL {
        fetch_model_bundle(client, artifact, output_dir).await
    } else {
        fetch_single_document(client, artifact, output_dir).await
    }
}

async fn fetch_model_bundle(
    client: &reqwest::Client,
    artifact: &DiscoveredArtifact,
    output_dir: &Path,
) -> Result<LocalArtifact> {
    let bundle_dir = output_dir.join(sanitize_part(&artifact.metadata.artifact_id));
    fs::create_dir_all(&bundle_dir)?;

    let toc_html = download_text(client, mms_parser::TOC_URL).await?;
    fs::write(bundle_dir.join("_toc.htm"), toc_html.as_bytes())?;

    let toc_entries = mms_parser::parse_toc(&toc_html)?;
    let mut files: Vec<(String, Vec<u8>)> = vec![("_toc.htm".to_string(), toc_html.into_bytes())];

    for entry in &toc_entries {
        for filename in &entry.page_files {
            let url = format!("{}/{}", mms_parser::BASE_URL, filename);
            let bytes = download_bytes(client, &url).await?;
            fs::write(bundle_dir.join(filename), &bytes)?;
            files.push((filename.clone(), bytes));
        }
    }

    files.sort_by(|a, b| a.0.cmp(&b.0));
    let mut hasher = Sha256::new();
    let mut total_len = 0u64;
    for (name, bytes) in &files {
        hasher.update(name.as_bytes());
        hasher.update(bytes);
        total_len += bytes.len() as u64;
    }

    let mut metadata = artifact.metadata.clone();
    metadata.fetched_at = Some(Utc::now());
    metadata.content_sha256 = Some(format!("{:x}", hasher.finalize()));
    metadata.content_length_bytes = Some(total_len);

    Ok(LocalArtifact {
        metadata,
        local_path: bundle_dir,
    })
}

async fn fetch_single_document(
    client: &reqwest::Client,
    artifact: &DiscoveredArtifact,
    output_dir: &Path,
) -> Result<LocalArtifact> {
    let bytes = download_bytes(client, &artifact.metadata.acquisition_uri).await?;
    let filename = local_filename(&artifact.metadata);
    let local_path = output_dir.join(filename);
    fs::write(&local_path, &bytes)?;

    let mut metadata = artifact.metadata.clone();
    metadata.fetched_at = Some(Utc::now());
    metadata.content_sha256 = Some(format!("{:x}", Sha256::digest(&bytes)));
    metadata.content_length_bytes = Some(bytes.len() as u64);

    Ok(LocalArtifact {
        metadata,
        local_path,
    })
}

async fn download_text(client: &reqwest::Client, url: &str) -> Result<String> {
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("downloading {url}"))?;
    if response.status() != StatusCode::OK {
        return Err(anyhow!("download failed for {url}: {}", response.status()));
    }
    response
        .text()
        .await
        .with_context(|| format!("reading {url}"))
}

async fn download_bytes(client: &reqwest::Client, url: &str) -> Result<Vec<u8>> {
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("downloading {url}"))?;
    if response.status() != StatusCode::OK {
        return Err(anyhow!("download failed for {url}: {}", response.status()));
    }
    Ok(response.bytes().await?.to_vec())
}

fn local_filename(metadata: &ArtifactMetadata) -> String {
    let extension =
        extension_for_uri(&metadata.acquisition_uri).unwrap_or_else(|| "bin".to_string());
    format!("{}.{}", sanitize_part(&metadata.artifact_id), extension)
}

fn extension_for_uri(uri: &str) -> Option<String> {
    PathBuf::from(uri.rsplit('/').next()?)
        .extension()
        .and_then(|ext| ext.to_str())
        .map(str::to_string)
}
