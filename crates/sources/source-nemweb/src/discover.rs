use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use ingest_core::{
    ArtifactKind, ArtifactMetadata, DiscoveredArtifact, DiscoveryCursorHint, RunContext,
    SourceFamily,
};
use reqwest::Url;
use scraper::{Html, Selector};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone)]
struct ArchiveListing {
    file_name: String,
    url: Url,
    published_at: Option<DateTime<Utc>>,
}

pub async fn discover_recent_archives(
    client: &reqwest::Client,
    family: &SourceFamily,
    limit: usize,
    cursor: &DiscoveryCursorHint,
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

    discover_recent_archives_from_html(&html, family, limit, cursor, ctx)
}

fn discover_recent_archives_from_html(
    html: &str,
    family: &SourceFamily,
    limit: usize,
    cursor: &DiscoveryCursorHint,
    ctx: &RunContext,
) -> Result<Vec<DiscoveredArtifact>> {
    let document = Html::parse_document(html);
    let selector = Selector::parse("a").map_err(|_| anyhow!("failed to parse selector"))?;
    let base = Url::parse(&family.listing_url)?;

    let mut archives = document
        .select(&selector)
        .filter_map(|node| node.value().attr("href"))
        .filter(|href| href.ends_with(".zip"))
        .filter_map(|href| {
            let url = base.join(href).ok()?;
            let file_name = href.rsplit('/').next()?.to_string();
            Some(ArchiveListing {
                published_at: infer_published_at_from_release_name(&file_name),
                file_name,
                url,
            })
        })
        .collect::<Vec<_>>();

    archives.sort_by(|left, right| left.file_name.cmp(&right.file_name));
    archives.reverse();

    let selected = select_archives_for_cursor(&archives, limit, cursor);
    Ok(selected
        .into_iter()
        .map(|archive| DiscoveredArtifact {
            metadata: ArtifactMetadata {
                artifact_id: stable_artifact_id(&family.id, archive.url.as_str()),
                source_id: family.id.to_string(),
                acquisition_uri: archive.url.to_string(),
                discovered_at: Utc::now(),
                fetched_at: None,
                published_at: archive.published_at,
                content_sha256: None,
                content_length_bytes: None,
                kind: ArtifactKind::ZipArchive,
                parser_version: ctx.parser_version.clone(),
                model_version: None,
                release_name: Some(archive.file_name),
            },
        })
        .collect())
}

fn select_archives_for_cursor(
    archives: &[ArchiveListing],
    limit: usize,
    cursor: &DiscoveryCursorHint,
) -> Vec<ArchiveListing> {
    if limit == 0 || archives.is_empty() {
        return Vec::new();
    }

    if cursor.latest_release_name.is_none() && cursor.earliest_release_name.is_none() {
        return archives.iter().take(limit).cloned().collect();
    }

    let mut selected = Vec::with_capacity(limit);

    if let Some(latest_release_name) = cursor.latest_release_name.as_deref() {
        for archive in archives
            .iter()
            .filter(|archive| archive.file_name.as_str() > latest_release_name)
        {
            selected.push(archive.clone());
            if selected.len() >= limit {
                return selected;
            }
        }
    }

    if let Some(earliest_release_name) = cursor.earliest_release_name.as_deref() {
        for archive in archives
            .iter()
            .filter(|archive| archive.file_name.as_str() < earliest_release_name)
        {
            selected.push(archive.clone());
            if selected.len() >= limit {
                return selected;
            }
        }
    }

    selected
}

fn stable_artifact_id(family_id: &str, acquisition_uri: &str) -> String {
    let digest = Sha256::digest(acquisition_uri.as_bytes());
    format!("{family_id}-{:x}", digest)
}

fn infer_published_at_from_release_name(release_name: &str) -> Option<DateTime<Utc>> {
    for digit_run in release_name.split(|ch: char| !ch.is_ascii_digit()) {
        match digit_run.len() {
            14 => {
                if let Ok(parsed) = NaiveDateTime::parse_from_str(digit_run, "%Y%m%d%H%M%S") {
                    return Some(Utc.from_utc_datetime(&parsed));
                }
            }
            12 => {
                if let Ok(parsed) = NaiveDateTime::parse_from_str(digit_run, "%Y%m%d%H%M") {
                    return Some(Utc.from_utc_datetime(&parsed));
                }
            }
            8 => {
                if let Ok(parsed) = NaiveDate::parse_from_str(digit_run, "%Y%m%d") {
                    let naive = parsed.and_hms_opt(0, 0, 0)?;
                    return Some(Utc.from_utc_datetime(&naive));
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn archive(file_name: &str) -> ArchiveListing {
        ArchiveListing {
            file_name: file_name.to_string(),
            published_at: infer_published_at_from_release_name(file_name),
            url: Url::parse(&format!("https://example.com/{file_name}")).unwrap(),
        }
    }

    #[test]
    fn selects_newest_when_cursor_empty() {
        let archives = vec![
            archive("A_20260101000000.zip"),
            archive("A_20251201000000.zip"),
        ];
        let selected = select_archives_for_cursor(&archives, 1, &DiscoveryCursorHint::default());
        assert_eq!(selected[0].file_name, "A_20260101000000.zip");
    }

    #[test]
    fn selects_older_unseen_archives_for_backfill() {
        let archives = vec![
            archive("A_20260105000000.zip"),
            archive("A_20260104000000.zip"),
            archive("A_20260103000000.zip"),
            archive("A_20260102000000.zip"),
        ];
        let cursor = DiscoveryCursorHint {
            latest_release_name: Some("A_20260105000000.zip".to_string()),
            earliest_release_name: Some("A_20260104000000.zip".to_string()),
            ..DiscoveryCursorHint::default()
        };
        let selected = select_archives_for_cursor(&archives, 2, &cursor);
        assert_eq!(
            selected
                .into_iter()
                .map(|archive| archive.file_name)
                .collect::<Vec<_>>(),
            vec![
                "A_20260103000000.zip".to_string(),
                "A_20260102000000.zip".to_string()
            ]
        );
    }

    #[test]
    fn infers_timestamp_from_release_name() {
        let published_at = infer_published_at_from_release_name(
            "PUBLIC_SEVENDAYOUTLOOK_FULL_20260313130553_0000000507671299.zip",
        )
        .unwrap();
        assert_eq!(published_at.to_rfc3339(), "2026-03-13T13:05:53+00:00");
    }
}
