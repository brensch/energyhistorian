import json
import os
import subprocess
from pathlib import Path

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset


REPO_ROOT = Path(__file__).resolve().parents[2]
RUST_PACKAGE = "energyhistorian"


def _run_cli(*args: str) -> dict:
    command = ["cargo", "run", "--quiet", "--package", RUST_PACKAGE, "--", *args]
    clickhouse_url = os.environ.get("CLICKHOUSE_URL")
    clickhouse_database = os.environ.get("CLICKHOUSE_DATABASE", "energyhistorian")
    if clickhouse_url and args and args[0] == "ingest":
        command.extend(
            [
                "--clickhouse-url",
                clickhouse_url,
                "--clickhouse-database",
                clickhouse_database,
            ]
        )
    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
        env={**os.environ, "CARGO_TERM_COLOR": "never"},
    )
    return json.loads(result.stdout)


@asset(group_name="nemweb")
def nemweb_source_catalog() -> MaterializeResult:
    catalog = _run_cli("list-sources")
    recommendations = [
        entry for entry in catalog if entry["recommendation_rank"] <= 2
    ]
    return MaterializeResult(
        metadata={
            "source_count": len(catalog),
            "catalog": MetadataValue.json(catalog),
            "recommended_starting_point": MetadataValue.json(recommendations),
        }
    )


@asset(group_name="nemweb", deps=[nemweb_source_catalog])
def nemweb_tradingis_snapshot(context) -> MaterializeResult:
    manifest = _run_cli("ingest", "--source", "tradingis", "--limit", "4")
    return _manifest_result(context, manifest)


@asset(group_name="nemweb", deps=[nemweb_source_catalog])
def nemweb_dispatchis_snapshot(context) -> MaterializeResult:
    manifest = _run_cli("ingest", "--source", "dispatchis", "--limit", "4")
    return _manifest_result(context, manifest)


def _manifest_result(context, manifest: dict) -> MaterializeResult:
    context.log.info("Ingested %s archives for %s", len(manifest["archives"]), manifest["source"])
    table_rows = {
        item["table_name"]: item["rows_written"] for item in manifest["tables"]
    }
    output_paths = {
        item["table_name"]: MetadataValue.path(item["output_path"])
        for item in manifest["tables"]
    }
    metadata = {
        "source": manifest["source"],
        "archive_count": len(manifest["archives"]),
        "tables": MetadataValue.json(manifest["tables"]),
        "next_sources": MetadataValue.json(manifest["proposed_next_sources"]),
        "clickhouse_tables": MetadataValue.json(
            [
                {
                    "table_name": item["table_name"],
                    "clickhouse_table": item["clickhouse_table"],
                    "rows_loaded": item["rows_loaded"],
                }
                for item in manifest["tables"]
            ]
        ),
    }
    metadata.update({f"rows_{name}": rows for name, rows in table_rows.items()})
    metadata.update({f"path_{name}": path for name, path in output_paths.items()})
    return MaterializeResult(metadata=metadata)
