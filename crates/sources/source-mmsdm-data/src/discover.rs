// ── MMSDM Discovery ──────────────────────────────────────────────────────
//
// Discovery for the MMSDM monthly archive works in two phases:
//
// Phase 1 — Skeleton crawl (fast, ~19 HTTP requests):
//   Fetches the root archive page and all year pages *concurrently* to
//   build a sorted list of every available year/month slot.  Each slot's
//   DATA/ URL is constructed deterministically from the directory name.
//   This completes in a few seconds.
//
// Phase 2 — Month batch (bounded, `limit` HTTP requests):
//   For months not yet covered by the cursor, fetches the DATA/ directory
//   listing and extracts matching zip files.  Only processes `limit` months
//   per call; the orchestrator will call back in ~10 seconds if there's
//   more to do.
//
// This means first-run discovery smears itself across many short calls
// instead of blocking for 5+ minutes on one giant sequential crawl.

use anyhow::{Context, Result};
use chrono::Utc;
use futures_util::future::join_all;
use ingest_core::{
    ArtifactKind, ArtifactMetadata, DiscoveredArtifact, DiscoveryCursorHint, RunContext,
};
use regex::Regex;

/// Root URL for the MMSDM monthly archive on nemweb.
const MMSDM_ARCHIVE_ROOT: &str = "https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/";
/// Base URL for constructing absolute hrefs from relative links.
const NEMWEB_ROOT: &str = "https://nemweb.com.au";

pub const SOURCE_ID: &str = "aemo.mmsdm.data";
pub const COLLECTION_ID: &str = "public-reference-data";

/// A discovered month slot with its precomputed DATA directory URL.
///
/// Built during the skeleton crawl.  The year/month fields are used for
/// cursor filtering; the data_url is the page we fetch to find zip files.
struct MonthSlot {
    year: i32,
    month: u32,
    /// Release key like "2024-01", used as the artifact release_name.
    month_key: String,
    /// Full URL to the DATA/ directory for this month.
    data_url: String,
}

/// Top-level discovery entry point.
///
/// 1. Builds the month schedule (skeleton crawl).
/// 2. Filters by cursor to find months that haven't been discovered yet.
/// 3. Fetches DATA/ listings for up to `limit` months.
/// 4. Returns discovered artifacts.
pub async fn discover_public_reference_data(
    client: &reqwest::Client,
    limit: usize,
    cursor: &DiscoveryCursorHint,
    ctx: &RunContext,
) -> Result<Vec<DiscoveredArtifact>> {
    let href_re = Regex::new(r#"HREF="([^"]+)""#)?;
    let zip_re = Regex::new(r#"PUBLIC_ARCHIVE#([A-Z0-9_]+)#FILE(\d+)#([0-9]{6,12})\.zip"#)?;

    // Phase 1: skeleton crawl — build the full month schedule.
    let month_slots = build_month_schedule(client).await?;
    tracing::info!(
        total_months = month_slots.len(),
        "mmsdm: skeleton crawl complete"
    );

    // Filter to months not yet covered by cursor.
    // The cursor's latest_release_name is e.g. "2024-01"; we include that
    // month and everything after it (re-check the latest known month in case
    // new files were added to it).
    let earliest_month = cursor
        .latest_release_name
        .as_deref()
        .and_then(parse_release_month);

    let pending = select_pending_months(month_slots, earliest_month);

    tracing::info!(
        pending = pending.len(),
        cursor = ?earliest_month,
        "mmsdm: months to discover"
    );

    // Phase 2: fetch DATA/ for a batch of months.
    // We limit by number of months crawled, not artifacts, so the HTTP cost
    // per discovery call is bounded and predictable.
    let batch_month_limit = limit.max(1);
    let mut artifacts = Vec::new();
    let mut months_crawled = 0usize;

    for slot in &pending {
        if months_crawled >= batch_month_limit {
            break;
        }
        let month_artifacts = discover_month_artifacts(
            client,
            &slot.data_url,
            &slot.month_key,
            ctx,
            &href_re,
            &zip_re,
        )
        .await?;
        artifacts.extend(month_artifacts);
        months_crawled += 1;
    }

    artifacts.sort_by(|left, right| left.metadata.artifact_id.cmp(&right.metadata.artifact_id));
    tracing::info!(
        months_crawled,
        total_pending = pending.len(),
        artifacts = artifacts.len(),
        "mmsdm: discovery batch complete"
    );

    Ok(artifacts)
}

fn select_pending_months(
    month_slots: Vec<MonthSlot>,
    earliest_month: Option<(i32, u32)>,
) -> Vec<MonthSlot> {
    let mut pending: Vec<_> = month_slots
        .into_iter()
        .filter(|slot| {
            if let Some((cursor_year, cursor_month)) = earliest_month {
                (slot.year, slot.month) >= (cursor_year, cursor_month)
            } else {
                true
            }
        })
        .collect();
    pending.sort_by_key(|slot| std::cmp::Reverse((slot.year, slot.month)));
    pending
}

/// Fetch the root and all year pages to build a sorted list of month slots.
///
/// This is the "skeleton crawl": 1 request for the root page, then all year
/// pages fetched concurrently (~18 requests).  The result is a complete
/// picture of which months exist in the archive.
///
/// Year pages are fetched concurrently via `join_all` because they are
/// independent and each is a tiny HTML listing (~1 KB).
async fn build_month_schedule(client: &reqwest::Client) -> Result<Vec<MonthSlot>> {
    let year_re = Regex::new(r#"HREF="[^"]*?(\d{4})/""#)?;
    let month_re = Regex::new(r#"(MMSDM_(\d{4})_(\d{2}))/"#)?;

    // Fetch root to discover year directories.
    let root_html = fetch_text(client, MMSDM_ARCHIVE_ROOT).await?;
    let mut years: Vec<String> = year_re
        .captures_iter(&root_html)
        .filter_map(|captures| captures.get(1).map(|m| m.as_str().to_string()))
        .collect();
    years.sort();
    years.dedup();
    tracing::info!(count = years.len(), "mmsdm: discovered year directories");

    // Fetch all year pages concurrently to extract month directories.
    let year_futures: Vec<_> = years
        .iter()
        .map(|year| {
            let url = format!("{MMSDM_ARCHIVE_ROOT}{year}/");
            async move {
                let html = fetch_text(client, &url).await;
                (year.clone(), html)
            }
        })
        .collect();
    let year_results = join_all(year_futures).await;

    // Parse each year page to extract month slots.
    let mut slots = Vec::new();
    for (year, html_result) in year_results {
        let year_html = match html_result {
            Ok(html) => html,
            Err(err) => {
                tracing::warn!(%year, %err, "mmsdm: failed to fetch year page, skipping");
                continue;
            }
        };
        for captures in month_re.captures_iter(&year_html) {
            let Some(month_dir) = captures.get(1).map(|m| m.as_str()) else {
                continue;
            };
            let Some(y) = captures.get(2).and_then(|m| m.as_str().parse::<i32>().ok()) else {
                continue;
            };
            let Some(m) = captures.get(3).and_then(|m| m.as_str().parse::<u32>().ok()) else {
                continue;
            };
            slots.push(MonthSlot {
                year: y,
                month: m,
                month_key: format!("{y}-{m:02}"),
                // Construct the full DATA/ URL deterministically from the
                // directory name.  This is always:
                //   {root}{year}/{month_dir}/MMSDM_Historical_Data_SQLLoader/DATA/
                data_url: format!(
                    "{MMSDM_ARCHIVE_ROOT}{year}/{month_dir}/MMSDM_Historical_Data_SQLLoader/DATA/"
                ),
            });
        }
    }

    slots.sort_by_key(|s| (s.year, s.month));
    slots.dedup_by_key(|s| (s.year, s.month));
    Ok(slots)
}

/// Fetch a single month's DATA/ directory and extract matching zip artifacts.
///
/// Parses the directory listing HTML to find every DATA zip that matches the
/// public archive filename pattern. Each zip filename encodes the table name,
/// file number, and a publication timestamp.
///
/// If the DATA/ directory doesn't exist (404 or other error), we log a
/// warning and return an empty list rather than failing the entire discovery.
async fn discover_month_artifacts(
    client: &reqwest::Client,
    data_url: &str,
    month_key: &str,
    ctx: &RunContext,
    href_re: &Regex,
    zip_re: &Regex,
) -> Result<Vec<DiscoveredArtifact>> {
    // Fetch the DATA/ directory listing.  Some months (especially the
    // current/most-recent month) might not have a DATA/ directory yet.
    let data_html = match fetch_text(client, data_url).await {
        Ok(html) => html,
        Err(err) => {
            tracing::warn!(month_key, %err, "mmsdm: failed to fetch DATA directory, skipping month");
            return Ok(Vec::new());
        }
    };

    Ok(extract_month_artifacts(
        &data_html, data_url, month_key, ctx, href_re, zip_re,
    ))
}

fn extract_month_artifacts(
    data_html: &str,
    data_url: &str,
    month_key: &str,
    ctx: &RunContext,
    href_re: &Regex,
    zip_re: &Regex,
) -> Vec<DiscoveredArtifact> {
    // Extract all hrefs from the directory listing.
    let mut hrefs = href_re
        .captures_iter(data_html)
        .filter_map(|captures| captures.get(1).map(|m| m.as_str().to_string()))
        .collect::<Vec<_>>();
    hrefs.sort();
    hrefs.dedup();

    let mut artifacts = Vec::new();
    for href in hrefs {
        // Hrefs sometimes URL-encode the # as %23.
        let decoded_href = href.replace("%23", "#");
        let Some(filename) = decoded_href.rsplit('/').next() else {
            continue;
        };

        // Parse the zip filename to extract table name, file number, and
        // publication token.
        let Some(captures) = zip_re.captures(filename) else {
            continue;
        };
        let Some(table_name) = captures.get(1).map(|m| m.as_str()) else {
            continue;
        };

        let Some(file_no) = captures.get(2).map(|m| m.as_str()) else {
            continue;
        };
        let Some(published_token) = captures.get(3).map(|m| m.as_str()) else {
            continue;
        };

        // Build the full download URL from the href.
        let acquisition_uri = if href.starts_with("http") {
            href.clone()
        } else if href.starts_with('/') {
            format!("{NEMWEB_ROOT}{href}")
        } else {
            format!("{data_url}{href}")
        };

        artifacts.push(DiscoveredArtifact {
            metadata: ArtifactMetadata {
                artifact_id: format!(
                    "aemo_mmsdm_{month_key}_{table_name}_file{file_no}_{published_token}"
                ),
                source_id: SOURCE_ID.to_string(),
                acquisition_uri,
                discovered_at: Utc::now(),
                fetched_at: None,
                published_at: Some(Utc::now()),
                content_sha256: None,
                content_length_bytes: None,
                kind: ArtifactKind::ZipArchive,
                parser_version: ctx.parser_version.clone(),
                model_version: None,
                release_name: Some(month_key.to_string()),
            },
        });
    }
    artifacts
}

/// Fetch a URL as text with error context.
pub(crate) async fn fetch_text(client: &reqwest::Client, url: &str) -> Result<String> {
    client
        .get(url)
        .send()
        .await
        .with_context(|| format!("fetching {url}"))?
        .error_for_status()
        .with_context(|| format!("unexpected status fetching {url}"))?
        .text()
        .await
        .with_context(|| format!("reading body for {url}"))
}

/// Parse a release name like "2024-01" into (year, month).
fn parse_release_month(release_name: &str) -> Option<(i32, u32)> {
    let (year, month) = release_name.split_once('-')?;
    Some((year.parse().ok()?, month.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_month_artifacts_keeps_all_matching_tables() {
        let href_re = Regex::new(r#"HREF="([^"]+)""#).unwrap();
        let zip_re =
            Regex::new(r#"PUBLIC_ARCHIVE#([A-Z0-9_]+)#FILE(\d+)#([0-9]{6,12})\.zip"#).unwrap();
        let ctx = RunContext {
            run_id: "test-run".to_string(),
            environment: "test".to_string(),
            parser_version: "source-mmsdm-data/0.1".to_string(),
        };
        let html = r#"
            <a HREF="PUBLIC_ARCHIVE%23DUDETAILSUMMARY%23FILE01%23202401010000.zip">one</a>
            <a HREF="PUBLIC_ARCHIVE#SSM_ENABLEMENT_PERIOD#FILE02#202401010500.zip">two</a>
            <a HREF="notes.txt">ignore</a>
        "#;

        let artifacts = extract_month_artifacts(
            html,
            "https://nemweb.example/DATA/",
            "2024-01",
            &ctx,
            &href_re,
            &zip_re,
        );

        assert_eq!(artifacts.len(), 2);
        assert_eq!(artifacts[0].metadata.source_id, SOURCE_ID);
        assert!(
            artifacts
                .iter()
                .any(|artifact| artifact.metadata.artifact_id
                    == "aemo_mmsdm_2024-01_DUDETAILSUMMARY_file01_202401010000")
        );
        assert!(
            artifacts
                .iter()
                .any(|artifact| artifact.metadata.artifact_id
                    == "aemo_mmsdm_2024-01_SSM_ENABLEMENT_PERIOD_file02_202401010500")
        );
    }

    #[test]
    fn parse_release_month_parses_year_month() {
        assert_eq!(parse_release_month("2024-01"), Some((2024, 1)));
        assert_eq!(parse_release_month("bad"), None);
    }

    #[test]
    fn newest_months_are_prioritized_first() {
        let pending = select_pending_months(
            vec![
                MonthSlot {
                    year: 2025,
                    month: 1,
                    month_key: "2025-01".to_string(),
                    data_url: "one".to_string(),
                },
                MonthSlot {
                    year: 2026,
                    month: 2,
                    month_key: "2026-02".to_string(),
                    data_url: "two".to_string(),
                },
                MonthSlot {
                    year: 2025,
                    month: 12,
                    month_key: "2025-12".to_string(),
                    data_url: "three".to_string(),
                },
            ],
            None,
        );

        let ordered = pending
            .into_iter()
            .map(|slot| slot.month_key)
            .collect::<Vec<_>>();
        assert_eq!(ordered, vec!["2026-02", "2025-12", "2025-01"]);
    }

    #[test]
    fn cursor_keeps_recent_and_later_months_eligible_for_follow_up_backfill() {
        let pending = select_pending_months(
            vec![
                MonthSlot {
                    year: 2024,
                    month: 12,
                    month_key: "2024-12".to_string(),
                    data_url: "old".to_string(),
                },
                MonthSlot {
                    year: 2025,
                    month: 1,
                    month_key: "2025-01".to_string(),
                    data_url: "mid".to_string(),
                },
                MonthSlot {
                    year: 2025,
                    month: 12,
                    month_key: "2025-12".to_string(),
                    data_url: "new".to_string(),
                },
            ],
            Some((2025, 1)),
        );

        let ordered = pending
            .into_iter()
            .map(|slot| slot.month_key)
            .collect::<Vec<_>>();
        assert_eq!(ordered, vec!["2025-12", "2025-01"]);
    }

    #[test]
    fn no_cursor_means_older_months_remain_part_of_eventual_backfill_set() {
        let pending = select_pending_months(
            vec![
                MonthSlot {
                    year: 2024,
                    month: 12,
                    month_key: "2024-12".to_string(),
                    data_url: "old".to_string(),
                },
                MonthSlot {
                    year: 2025,
                    month: 1,
                    month_key: "2025-01".to_string(),
                    data_url: "mid".to_string(),
                },
                MonthSlot {
                    year: 2025,
                    month: 12,
                    month_key: "2025-12".to_string(),
                    data_url: "new".to_string(),
                },
            ],
            None,
        );

        let ordered = pending
            .into_iter()
            .map(|slot| slot.month_key)
            .collect::<Vec<_>>();
        assert_eq!(ordered, vec!["2025-12", "2025-01", "2024-12"]);
    }
}
