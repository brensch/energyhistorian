# AEMO Metadata Ingestion

## Short answer

Yes, you should ingest AEMO’s metadata documents as first-class source data.

For this platform, the metadata layer should not be handwritten notes or static markdown. It should be a versioned dataset built from AEMO’s own technical documentation and relationship tables.

## What AEMO provides

AEMO exposes several authoritative metadata sources that are directly useful.

## 1. Data Model Reports

These are the core table-definition documents.

They describe:

- tables
- columns
- data types
- primary keys
- comments / purpose
- visibility and data volume in some versions

Official source:

- AEMO says the Data Model reports explain all Data Model tables, and provides downloadable reports:
  - https://www.aemo.com.au/energy-systems/market-it-systems/electricity-system-guides/wholesale-it-systems-software
  - https://di-help.docs.public.aemo.com.au/Content/Data_Model/MMS_Data_Model.htm?TocPath=_____8

Example from the current Electricity Data Model v5.6 PDF:

- table comment
- field name
- data type
- primary key
- column comment

Source:

- https://tech-specs.docs.public.aemo.com.au/Content/TSP_EMMSDM56_Nov2025/EMMS_DM5.6_Nov_2025_v3.01_Markup.pdf

## 2. Package Summary

This is useful for package-level semantics and grouping tables by business domain.

It explains:

- packages
- associated tables
- brief summaries
- entity diagrams in the older document set

Source reference:

- https://www.aemo.com.au/energy-systems/market-it-systems/electricity-system-guides/wholesale-it-systems-software
- https://di-help.docs.public.aemo.com.au/Content/Data_Model/MMS_Data_Model.htm?TocPath=_____8

## 3. Table-to-Report Relationships

This is one of the most important metadata sources for ingestion design.

It maps:

- package ID
- data model table
- file ID
- file name
- report type
- report subtype
- report version
- transaction type
- row type filter

That is exactly the bridge between:

- AEMO report files
- logical tables
- your parser routing

Official source:

- https://di-help.docs.public.aemo.com.au/Content/Data_Subscription/TableToFileReport.htm

Important detail:

- AEMO says this view can be exported to CSV.

That means this should be ingested as structured metadata whenever possible rather than scraped from rendered HTML only.

## 4. Upgrade Reports

These explain schema and report changes between data model versions.

They are essential for drift handling because they describe:

- new tables
- modified tables
- discontinued reports
- replacement mappings

Source references:

- https://di-help.docs.public.aemo.com.au/Content/Data_Model/MMS_Data_Model.htm?TocPath=_____8
- https://tech-specs.docs.public.aemo.com.au/Content/TSP_EMMSDM56_Nov2025/EMMS_-_DM5.6_-_Nov_2025_1.htm?TocPath=Electricity+Data%C2%A0Model%7CDM+5.6+-+November+2025%7C_____4

## 5. Data Population Dates

This is operational metadata.

It tells you:

- which data model table went live when
- which file ID carries it
- which data model version it belongs to
- pre-production vs production dates

Source:

- https://tech-specs.docs.public.aemo.com.au/Content/TSP_TechnicalSpecificationPortal/Data_population_dates.htm

This is highly useful for:

- backfill planning
- expected availability windows
- determining whether missing data is genuinely missing or not yet introduced

## 6. Participant Impact / release notes

These capture real migration semantics that table definitions alone do not.

Example:

- renamed structures
- manually subscribed new reports
- backfill requirements after release changes

Source:

- https://tech-specs.docs.public.aemo.com.au/Content/TSP_EMMSDM53_April2024/Participant_Impact.htm

This is important because it often explains the operational consequences of schema/version changes more clearly than the pure table report.

## 7. Market Portal / NEMweb help pages

These help with:

- `C` / `I` / `D` CSV structure
- historical and archive behavior
- Data Model references

Source references:

- https://markets-portal-help.docs.public.aemo.com.au/Content/CSVdataFormat/CSV_File_format.htm
- https://markets-portal-help.docs.public.aemo.com.au/Content/EMMSenergyFCAS/DataModel.htm
- https://markets-portal-help.docs.public.aemo.com.au/Content/InformationSystems/Electricity/HistoricalData.htm

## What to ingest

Treat AEMO metadata as several datasets, not one blob.

## Recommended metadata datasets

### `aemo_meta.data_model_versions`

- model family: electricity/gas
- version
- release title
- release status
- published date
- source document URL

### `aemo_meta.packages`

- package ID
- package name
- package description
- model version

### `aemo_meta.tables`

- model version
- package ID
- table name
- table comment / purpose
- visibility if available
- data volume if available
- trigger/frequency if available
- source document URL

### `aemo_meta.columns`

- model version
- table name
- column ordinal
- column name
- data type
- primary key flag
- column comment
- source document URL

### `aemo_meta.table_report_relationships`

- model version if available
- package ID
- data model table
- file ID
- file name pattern
- report type
- report subtype
- report version
- transaction type
- row type filter
- source document URL

### `aemo_meta.upgrade_changes`

- from version
- to version
- change class: new/modified/discontinued/replaced
- package ID
- table name
- file ID
- prior report mapping
- replacement mapping
- notes

### `aemo_meta.population_dates`

- table name
- file ID
- data model version
- preprod date
- prod date
- notes

### `aemo_meta.document_catalog`

- document type
- title
- URL
- version
- publication date
- content hash
- fetched at
- parser version

## How to ingest it

## Priority order

### First-class structured sources

Prefer these first:

- HTML tables
- CSV exports where AEMO provides them
- structured release pages

Why:

- easier to diff
- fewer OCR/PDF parsing errors
- better column fidelity

### PDF as fallback and archival source

Use PDFs for:

- exact version retention
- fields not available elsewhere
- human-readable comments
- historical versions

But do not rely on PDF parsing alone if an HTML or CSV view exists.

## Practical ingestion strategy

For each metadata source:

1. fetch raw document
2. store immutable artifact
3. detect document type
4. parse into normalized metadata tables
5. compute structural hash
6. compare against previous version
7. emit change events

## Parser types you need

### HTML table parser

Use for:

- Data Population Dates
- web technical spec pages
- relationship pages

### CSV importer

Use when AEMO provides export CSV from relationship screens.

This should become the preferred source for `table_report_relationships`.

### PDF table extractor

Use for:

- Data Model Report PDFs
- Upgrade Report PDFs
- Package Summary PDFs

You should expect cleanup logic because PDF table extraction is never completely clean.

## How this fits your Rust service

The metadata ingestion should be implemented as another source family, not bolted on as docs scraping.

Recommended plugins:

- `aemo.meta.data_model_reports`
- `aemo.meta.package_summaries`
- `aemo.meta.table_to_report_relationships`
- `aemo.meta.population_dates`
- `aemo.meta.upgrade_reports`
- `aemo.meta.participant_impact`

That means the same ingestion framework can handle:

- raw artifacts
- parser versioning
- schema tracking
- change detection

## Why this matters for the main data ingest

This metadata is not just documentation. It drives the parser and promotion engine.

It can answer:

- which logical tables should exist in a file family
- which file IDs map to which table names
- what the expected columns are
- whether a report version changed legitimately
- whether a new table is expected because of a new data model release
- when a table should first appear historically

Without this, you are guessing too much from live files.

## Recommended production use

## Use metadata in three places

### 1. Parser routing

Use `table_report_relationships` to determine:

- expected report families
- table-to-file mappings
- report version mappings

### 2. Schema validation

Use `columns` and `upgrade_changes` to compare observed `I` headers against expected structures.

### 3. LLM-facing catalog

Use `tables`, `columns`, `packages`, and `upgrade_changes` to populate:

- dataset descriptions
- business meaning
- join hints
- lineage notes
- known replacements / deprecated tables

This is one of the best sources you have for high-quality metadata for the portal.

## Implementation recommendation

### First milestone

Ingest these first:

- `Data Model Report v5.6`
- `Package Summary v5.6`
- `Table to report relationships`
- `Data Population Dates`
- `Upgrade Report v5.6`

That gives you enough to build:

- a metadata registry
- file-to-table routing
- first schema validation rules

### Second milestone

Add prior versions:

- v5.5
- v5.3
- historical PDFs you can access

This gives you version history and migration context.

### Third milestone

Tie metadata directly into promotion policy:

- block promotion on unknown schema
- allow promotion on approved additive changes
- surface release-linked warnings in Dagster asset checks

## Bottom line

You should ingest AEMO metadata documents as a formal source domain.

The highest-value pieces are:

- Data Model Report
- Table-to-report relationships
- Upgrade Report
- Data Population Dates

Together, those give you:

- table definitions
- field definitions
- file-to-table mappings
- release-aware schema change handling
- better metadata for LLMs

That is the right way to keep the ingestion system and the analytical catalog aligned with AEMO’s moving source estate.

## Sources

- Wholesale IT systems software:
  - https://www.aemo.com.au/energy-systems/market-it-systems/electricity-system-guides/wholesale-it-systems-software
- Data Model reports index:
  - https://di-help.docs.public.aemo.com.au/Content/Data_Model/MMS_Data_Model.htm?TocPath=_____8
- Table to report relationships:
  - https://di-help.docs.public.aemo.com.au/Content/Data_Subscription/TableToFileReport.htm
- Data population dates:
  - https://tech-specs.docs.public.aemo.com.au/Content/TSP_TechnicalSpecificationPortal/Data_population_dates.htm
- Electricity Data Model v5.6:
  - https://tech-specs.docs.public.aemo.com.au/Content/TSP_EMMSDM56_Nov2025/EMMS_-_DM5.6_-_Nov_2025_1.htm?TocPath=Electricity+Data%C2%A0Model%7CDM+5.6+-+November+2025%7C_____4
- Electricity Data Model v5.6 PDF:
  - https://tech-specs.docs.public.aemo.com.au/Content/TSP_EMMSDM56_Nov2025/EMMS_DM5.6_Nov_2025_v3.01_Markup.pdf
- Participant impact example:
  - https://tech-specs.docs.public.aemo.com.au/Content/TSP_EMMSDM53_April2024/Participant_Impact.htm
- Data model help:
  - https://markets-portal-help.docs.public.aemo.com.au/Content/EMMSenergyFCAS/DataModel.htm
- CSV format help:
  - https://markets-portal-help.docs.public.aemo.com.au/Content/CSVdataFormat/CSV_File_format.htm
- Historical data help:
  - https://markets-portal-help.docs.public.aemo.com.au/Content/InformationSystems/Electricity/HistoricalData.htm
