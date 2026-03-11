//! Fetches and parses the AEMO MMS Data Model into a JSON catalog.
//! Usage: cargo run -p source-aemo-metadata --bin fetch_catalog [output_path]

use anyhow::Result;
use source_aemo_metadata::AemoMetadataHtmlPlugin;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let output_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "data/catalog.json".to_string());

    let plugin = AemoMetadataHtmlPlugin::new();
    let client = reqwest::Client::builder()
        .user_agent("energyhistorian/0.1")
        .build()?;

    println!("Fetching MMS Data Model from AEMO...");
    let catalog = plugin.build_catalog(&client).await?;

    println!("Parsed:");
    println!("  {} packages", catalog.packages.len());
    println!(
        "  {} tables ({} total columns)",
        catalog.tables.len(),
        catalog
            .tables
            .iter()
            .map(|t| t.columns.len())
            .sum::<usize>()
    );
    println!("  {} concepts", catalog.concepts.len());
    println!("  {} FCAS markets", catalog.fcas_markets.len());
    println!("  {} interconnectors", catalog.interconnectors.len());

    // Print some sample tables
    println!("\nSample tables:");
    for table in catalog.tables.iter().take(10) {
        println!(
            "  [{}/{}] {} columns, PK: {:?}",
            table.package_id,
            table.table_name,
            table.columns.len(),
            table.primary_key_columns
        );
        for col in table.columns.iter().take(3) {
            println!(
                "    - {} ({}{}) {}",
                col.column_name,
                col.oracle_type,
                if col.is_mandatory { ", mandatory" } else { "" },
                if col.description.len() > 60 {
                    format!("{}...", &col.description[..60])
                } else {
                    col.description.clone()
                }
            );
        }
    }

    // Write to file
    let json = AemoMetadataHtmlPlugin::catalog_to_json(&catalog)?;

    if let Some(parent) = std::path::Path::new(&output_path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&output_path, &json)?;
    println!(
        "\nCatalog written to {} ({} bytes)",
        output_path,
        json.len()
    );

    Ok(())
}
