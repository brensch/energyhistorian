use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use ingest_core::{ArtifactKind, ArtifactMetadata, DiscoveredArtifact, RunContext, SourceFamily};
use reqwest::Url;
use scraper::{Html, Selector};

pub async fn discover_recent_archives(
    client: &reqwest::Client,
    family: &SourceFamily,
    limit: usize,
    ctx: &RunContext,
) -> Result<Vec<DiscoveredArtifact>> {
    let html = client
        .get(&family.listing_url)
        .send()
        .await
        .with_context(|| format!("fetching listing {}", family.listing_url))?
        .error_for_status()
        .with_context(|| format!("listing request failed for {}", family.listing_url))?
        .text()
        .await?;

    discover_recent_archives_from_html(&html, family, limit, ctx)
}

fn discover_recent_archives_from_html(
    html: &str,
    family: &SourceFamily,
    limit: usize,
    ctx: &RunContext,
) -> Result<Vec<DiscoveredArtifact>> {
    let document = Html::parse_document(html);
    let selector = Selector::parse("a").map_err(|_| anyhow!("failed to parse selector"))?;
    let base = Url::parse(&family.listing_url)?;

    let mut links = document
        .select(&selector)
        .filter_map(|node| node.value().attr("href"))
        .filter(|href| href.ends_with(".zip"))
        .filter_map(|href| {
            let url = base.join(href).ok()?;
            let file_name = href.rsplit('/').next()?.to_string();
            Some((file_name, url))
        })
        .collect::<Vec<_>>();

    links.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(links
        .into_iter()
        .rev()
        .take(limit)
        .enumerate()
        .map(|(idx, (_file_name, url))| DiscoveredArtifact {
            metadata: ArtifactMetadata {
                artifact_id: format!("{}-{}-{}", family.id, ctx.run_id, idx),
                source_id: family.id.to_string(),
                acquisition_uri: url.to_string(),
                discovered_at: Utc::now(),
                fetched_at: None,
                published_at: None,
                content_sha256: None,
                content_length_bytes: None,
                kind: ArtifactKind::ZipArchive,
                parser_version: ctx.parser_version.clone(),
                model_version: None,
                release_name: None,
            },
        })
        .collect())
}
