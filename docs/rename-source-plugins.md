# Source Plugin Rename Implementation Guide

## Overview

Rename and restructure the four AEMO source plugins for clarity. The current
names are confusing — "DVD" is an internal AEMO term nobody recognises, and
"source-mmsdm" vs "source-aemo-metadata" don't clearly convey the data vs
metadata distinction.

### Naming Convention

All plugins follow the pattern `source-{origin}-{role}`:
- **origin**: where the data comes from (nemweb, mmsdm, aemo-docs)
- **role**: what it captures (data vs meta)

### Rename Table

| Current Crate | New Crate | Current source_id | New source_id | Current Raw DB | New Raw DB | What It Does |
|---|---|---|---|---|---|---|
| `source-nemweb` | `source-nemweb-data` | `aemo.nemweb` | `aemo.nemweb.data` | `raw_aemo_nemweb` | `raw_aemo_nemweb_data` | Operational data from NEMweb current reports (dispatch, bids, P5MIN zips) |
| `source-mmsdm` | `source-mmsdm-data` | `aemo.mmsdm` | `aemo.mmsdm.data` | `raw_aemo_mmsdm` | `raw_aemo_mmsdm_data` | Operational data from MMSDM monthly archive (all tables in DATA/) |
| `source-aemo-dvd` | `source-mmsdm-meta` | `aemo_metadata_dvd` | `aemo.mmsdm.meta` | `raw_aemo_metadata_dvd` | `raw_aemo_mmsdm_meta` | Schema DDL from MMSDM monthly archive (DOCUMENTATION/MMS Data Model/) |
| `source-aemo-metadata` | `source-aemo-docs` | `aemo_metadata_html` | `aemo.docs` | `raw_aemo_metadata_html` | `raw_aemo_docs` | AEMO HTML documentation: data model report, population dates, report mappings |

The raw database name is derived automatically: `raw_{sanitize(source_id)}`.
`sanitize()` lowercases and replaces non-alphanumeric chars with `_`.
So `aemo.mmsdm.data` becomes `raw_aemo_mmsdm_data`.

---

## Behavioural Change: Remove Static Table Filter from mmsdm-data

The current `source-mmsdm` has a static allow-list of 73 tables (`tables.rs`).
This should be removed. The data plugin should ingest **every** zip from
every `DATA/` directory. The table name is already captured in the artifact
metadata and the parser handles any CID CSV generically.

**Changes:**
- Delete `tables.rs` entirely
- In `discover.rs`, remove the `include_table_names()` check —
  accept all zip files matching the `PUBLIC_ARCHIVE#...#FILE...#....zip`
  pattern
- In `lib.rs`, remove `mod tables` and any references

This means ~200 tables per month instead of 73, but the discovery/fetch/parse
pipeline handles this fine — it's the same zip format regardless.

---

## File-by-File Change List

### 1. Rename crate directories

```
crates/sources/source-nemweb/      -> crates/sources/source-nemweb-data/
crates/sources/source-mmsdm/       -> crates/sources/source-mmsdm-data/
crates/sources/source-aemo-dvd/    -> crates/sources/source-mmsdm-meta/
crates/sources/source-aemo-metadata/ -> crates/sources/source-aemo-docs/
```

### 2. Workspace Cargo.toml (`/Cargo.toml`)

Update workspace members list (lines 11-14):
```toml
# Old
"crates/sources/source-aemo-dvd",
"crates/sources/source-aemo-metadata",
"crates/sources/source-mmsdm",
"crates/sources/source-nemweb",

# New
"crates/sources/source-aemo-docs",
"crates/sources/source-mmsdm-data",
"crates/sources/source-mmsdm-meta",
"crates/sources/source-nemweb-data",
```

Update dependencies (lines 29-32):
```toml
# Old
source-aemo-dvd = { path = "crates/sources/source-aemo-dvd" }
source-aemo-metadata = { path = "crates/sources/source-aemo-metadata" }
source-mmsdm = { path = "crates/sources/source-mmsdm" }
source-nemweb = { path = "crates/sources/source-nemweb" }

# New
source-aemo-docs = { path = "crates/sources/source-aemo-docs" }
source-mmsdm-data = { path = "crates/sources/source-mmsdm-data" }
source-mmsdm-meta = { path = "crates/sources/source-mmsdm-meta" }
source-nemweb-data = { path = "crates/sources/source-nemweb-data" }
```

### 3. Individual Crate Cargo.toml Files

Each crate's `Cargo.toml` needs its `[package] name` updated:

| File | Old name | New name |
|---|---|---|
| `source-nemweb-data/Cargo.toml` | `source-nemweb` | `source-nemweb-data` |
| `source-mmsdm-data/Cargo.toml` | `source-mmsdm` | `source-mmsdm-data` |
| `source-mmsdm-meta/Cargo.toml` | `source-aemo-dvd` | `source-mmsdm-meta` |
| `source-aemo-docs/Cargo.toml` | `source-aemo-metadata` | `source-aemo-docs` |

**Also**: `source-mmsdm-data/Cargo.toml` has a dependency on `source-nemweb`
(line 15) — update to `source-nemweb-data`.

### 4. Source Registry (`/src/source_registry.rs`)

Lines 15-18 — update import paths:
```rust
// Old
use source_aemo_dvd::AemoMetadataDvdPlugin;
use source_aemo_metadata::AemoMetadataHtmlPlugin;
use source_mmsdm::MmsdmPlugin;
use source_nemweb::NemwebPlugin;

// New
use source_mmsdm_meta::MmsdmMetaPlugin;
use source_aemo_docs::AemoDocsPlugin;
use source_mmsdm_data::MmsdmDataPlugin;
use source_nemweb_data::NemwebDataPlugin;
```

Lines 63-66 — update instantiation:
```rust
// Old
registry.register(NemwebPlugin::new());
registry.register(MmsdmPlugin::new());
registry.register(AemoMetadataHtmlPlugin::new());
registry.register(AemoMetadataDvdPlugin::new());

// New
registry.register(NemwebDataPlugin::new());
registry.register(MmsdmDataPlugin::new());
registry.register(AemoDocsPlugin::new());
registry.register(MmsdmMetaPlugin::new());
```

### 5. Plugin Struct Renames

| Crate | Old struct name | New struct name |
|---|---|---|
| source-nemweb-data | `NemwebPlugin` | `NemwebDataPlugin` |
| source-mmsdm-data | `MmsdmPlugin` | `MmsdmDataPlugin` |
| source-mmsdm-meta | `AemoMetadataDvdPlugin` | `MmsdmMetaPlugin` |
| source-aemo-docs | `AemoMetadataHtmlPlugin` | `AemoDocsPlugin` |

### 6. source_id Constants

Each plugin's `descriptor()` method returns a `source_id`. Update:

| Crate | File/line | Old value | New value |
|---|---|---|---|
| source-nemweb-data | `lib.rs` descriptor | `"aemo.nemweb"` | `"aemo.nemweb.data"` |
| source-mmsdm-data | `discover.rs:33` | `"aemo.mmsdm"` | `"aemo.mmsdm.data"` |
| source-mmsdm-data | `semantic.rs` (6 occurrences) | `"aemo.mmsdm"` | `"aemo.mmsdm.data"` |
| source-mmsdm-meta | `lib.rs:96` | `"aemo_metadata_dvd"` | `"aemo.mmsdm.meta"` |
| source-mmsdm-meta | `lib.rs:342` (artifact creation) | `"aemo_metadata_dvd"` | `"aemo.mmsdm.meta"` |
| source-aemo-docs | `lib.rs:82` | `"aemo_metadata_html"` | `"aemo.docs"` |
| source-aemo-docs | `discovery.rs:70,99` (artifact creation) | `"aemo_metadata_html"` | `"aemo.docs"` |

### 7. Parser Version Strings

Each plugin has a `parser_version()` method. Update:

| Crate | Old value | New value |
|---|---|---|
| source-nemweb-data | `"source-nemweb/0.1"` | `"source-nemweb-data/0.1"` |
| source-mmsdm-data | `"source-mmsdm/0.1"` | `"source-mmsdm-data/0.1"` |
| source-mmsdm-meta | `"source-aemo-dvd/0.1"` | `"source-mmsdm-meta/0.1"` |
| source-aemo-docs | `"source-aemo-metadata/0.1"` | `"source-aemo-docs/0.1"` |

### 8. Raw Database Name References in Semantic Views

These are hardcoded ClickHouse database names in SQL strings. The database
name is normally derived from source_id automatically, but some semantic
views reference other plugins' databases explicitly.

**source-mmsdm-data (`semantic.rs`):**
- References to `raw_aemo_mmsdm` → change to `raw_aemo_mmsdm_data`
  (lines ~263, 285, 288, 290, 304, 310)

**source-mmsdm-meta (`lib.rs`):**
- References to `raw_aemo_metadata_dvd` → change to `raw_aemo_mmsdm_meta`
  (lines 199, 200, 205, 206)

**source-aemo-docs (`lib.rs`):**
- References to `raw_aemo_metadata_html` → change to `raw_aemo_docs`
  (lines 206, 207, 212, 213, 218, 219)

**source-nemweb-data (`lib.rs`):**
- References to `raw_aemo_nemweb` → change to `raw_aemo_nemweb_data`
  (lines 571, 572, 577, 578)

### 9. Cross-Source Semantic View References

Several semantic views union data across sources. They reference each
other's view names by convention `{source_short}_table_locator`, etc.

**In source-mmsdm-data `semantic.rs`:**
```
semantic.nemweb_table_locator     (stays the same — this is a view name, not a DB)
semantic.mmsdm_table_locator      (stays the same)
semantic.nemweb_schema_registry   (stays the same)
semantic.mmsdm_schema_registry    (stays the same)
semantic.nemweb_model_registry    (stays the same)
semantic.mmsdm_model_registry     (stays the same)
```
These are view names in the `semantic` database — they don't need to change
unless you also want to rename the semantic views themselves. Recommend
leaving them as-is since they're user-facing names in the semantic layer.

**In source-nemweb-data `lib.rs` (lines 591, 600, 609):**
Same cross-references — leave the semantic view names unchanged.

### 10. Compose and Scripts

**`compose.yaml` line 89:**
```yaml
# Old
RAW_DATABASES: "${CLICKHOUSE_RAW_DATABASES:-raw_aemo_nemweb,raw_aemo_mmsdm,raw_aemo_metadata_dvd,raw_aemo_metadata_html}"

# New
RAW_DATABASES: "${CLICKHOUSE_RAW_DATABASES:-raw_aemo_nemweb_data,raw_aemo_mmsdm_data,raw_aemo_mmsdm_meta,raw_aemo_docs}"
```

**`scripts/provision_clickhouse_users.sh` line 16:**
```bash
# Old
"${RAW_DATABASES:=raw_aemo_nemweb,raw_aemo_mmsdm,raw_aemo_metadata_dvd,raw_aemo_metadata_html}"

# New
"${RAW_DATABASES:=raw_aemo_nemweb_data,raw_aemo_mmsdm_data,raw_aemo_mmsdm_meta,raw_aemo_docs}"
```

### 11. Internal Cross-Crate References

**source-mmsdm-data depends on source-nemweb for parsing:**
```rust
// In source-mmsdm-data lib.rs — these use the crate, not the source_id
source_nemweb::parse::parse_local_archive(...)
source_nemweb::parse::inspect_local_archive(...)
source_nemweb::parse::stream_local_archive_rows(...)
source_nemweb::parse::stream_local_archive_events(...)
```
After rename, these become `source_nemweb_data::parse::...` (Rust converts
hyphens to underscores in crate names).

### 12. Test and Harness References

**`/src/plugin_harness.rs` line 53:**
```rust
// Old default source_id
"aemo_metadata_html"
// New
"aemo.docs"
```

**`/tests/plugin_harness_smoke.rs` line 9:**
```rust
// Old
"aemo_metadata_html"
// New
"aemo.docs"
```

### 13. Documentation and Scripts

**`docs/service-first-architecture.md` line 126:**
- References `aemo.nemweb` → `aemo.nemweb.data`
- References `aemo.dvd` → `aemo.mmsdm.meta`

**`scripts/ask_nem.py` (lines 81-211):**
- Multiple references to `aemo.mmsdm` → `aemo.mmsdm.data`
- Multiple references to `aemo.nemweb` → `aemo.nemweb.data`

**`aemo-market-questions.md`:**
- References to `raw_aemo_mmsdm` → `raw_aemo_mmsdm_data`

**`PLAN-ai-ready-metadata.md`:**
- References to DVD → update terminology

---

## Data Migration Considerations

Changing source_id values means existing SQLite artifacts and ClickHouse raw
databases will be orphaned. Options:

### Option A: Clean Slate (recommended for dev)
Drop all existing data and re-ingest:
1. Delete `data/historian.db` (SQLite orchestration state)
2. Drop old ClickHouse raw databases
3. Restart — discovery will rebuild from scratch

### Option B: Migration (for production)
1. Rename ClickHouse databases:
   ```sql
   RENAME DATABASE raw_aemo_nemweb TO raw_aemo_nemweb_data;
   RENAME DATABASE raw_aemo_mmsdm TO raw_aemo_mmsdm_data;
   RENAME DATABASE raw_aemo_metadata_dvd TO raw_aemo_mmsdm_meta;
   RENAME DATABASE raw_aemo_metadata_html TO raw_aemo_docs;
   ```
2. Update SQLite source_id values:
   ```sql
   UPDATE artifacts SET source_id = 'aemo.nemweb.data' WHERE source_id = 'aemo.nemweb';
   UPDATE artifacts SET source_id = 'aemo.mmsdm.data' WHERE source_id = 'aemo.mmsdm';
   UPDATE artifacts SET source_id = 'aemo.mmsdm.meta' WHERE source_id = 'aemo_metadata_dvd';
   UPDATE artifacts SET source_id = 'aemo.docs' WHERE source_id = 'aemo_metadata_html';
   UPDATE schedules SET source_id = 'aemo.nemweb.data' WHERE source_id = 'aemo.nemweb';
   -- etc for all tables with source_id column
   UPDATE parse_runs SET parser_version = replace(parser_version, 'source-nemweb/', 'source-nemweb-data/');
   -- etc for all parser_version references
   ```
3. Recreate semantic views (they reference the raw database names)

---

## Verification Checklist

After all changes:
- [ ] `cargo check` passes with no errors
- [ ] `cargo test` passes
- [ ] All four plugins appear in the source registry on startup
- [ ] Discovery runs for all four sources
- [ ] ClickHouse raw databases are created with new names
- [ ] Semantic views reference correct raw database names
- [ ] `compose.yaml` provisions correct database names
- [ ] No remaining references to old names (grep for: `aemo_metadata_dvd`,
      `aemo_metadata_html`, `source-aemo-dvd`, `source-aemo-metadata`,
      `"aemo.mmsdm"`, `"aemo.nemweb"`, `raw_aemo_mmsdm"`, `raw_aemo_nemweb"`)
