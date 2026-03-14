use std::time::Duration;

use energyhistorian::plugin_harness::{PluginHarnessConfig, run_plugin_harness};

#[tokio::test]
#[ignore = "requires Docker plus live network access to AEMO endpoints"]
async fn plugin_harness_runs_metadata_plugin_end_to_end() {
    let snapshot = run_plugin_harness(PluginHarnessConfig {
        source_id: "aemo.docs".to_string(),
        collection_id: Some("population-dates".to_string()),
        run_for: Duration::from_secs(20),
        workdir: None,
        keep_runtime: false,
        clickhouse_image: "clickhouse/clickhouse-server:25.2".to_string(),
        preview_rows: 2,
    })
    .await
    .expect("plugin harness should complete successfully");

    assert!(
        snapshot
            .artifact_statuses
            .iter()
            .any(|status| status.status == "parsed" && status.count > 0),
        "expected at least one parsed artifact, got {:?}",
        snapshot.artifact_statuses
    );
    assert!(
        !snapshot.raw_tables.is_empty(),
        "expected ClickHouse raw tables to be populated"
    );
}
