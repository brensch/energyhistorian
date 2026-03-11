# Plan: AI-Ready Metadata for Energy Historian

## Problem Statement

nemexplorer (and any AI agent working with NEM data) needs to manually encode
domain knowledge that should come from the data platform itself:

| What nemexplorer manually encodes | Where it actually lives |
|---|---|
| 177 lines of table routing rules | MMS Data Model table/column descriptions |
| 365 DUID→fuel type hardcoded in Go | Registration tables + SCADA observation |
| Interconnector name→ID mappings | PARTICIPANT_REGISTRATION.INTERCONNECTOR |
| FCAS column locations & gotchas | MMS Data Model column descriptions |
| Revenue calculation formulas | MMS Data Model + domain knowledge |
| Valid enum values (SCADA_TYPE, DISPATCHTYPE) | MMS Data Model + observed distinct values |
| Column gotchas (DEMANDFORECAST isn't demand) | MMS Data Model descriptions + notes |
| Table relationships (what joins what) | MMS Data Model primary/foreign keys |
| Data freshness/availability | Population dates page + observed data ranges |

**Goal**: The historian's output should be self-describing enough that an AI agent
can discover everything it needs by querying the catalog — zero external rules.md.

---

## Architecture: The Catalog Database

A new `catalog` database in ClickHouse sits alongside `raw` data:

```
raw.*                   -- schema-hash-specific raw tables (existing)
catalog.packages        -- MMS packages (DISPATCH, BIDS, TRADING_DATA, etc.)
catalog.tables          -- table definitions with descriptions, update freq, visibility
catalog.columns         -- column definitions with descriptions, types, units, enums
catalog.relationships   -- foreign key and join relationships between tables
catalog.gotchas         -- known data quality issues, sentinel values, common mistakes
catalog.concepts        -- natural language → table/column mappings ("spot price" → dispatch__price.RRP)
catalog.reference_duids -- live DUID registry (fuel type, station, region, capacity, status)
catalog.reference_interconnectors -- interconnector definitions with from/to regions
catalog.reference_regions -- NEM region definitions
catalog.reference_fcas_markets -- FCAS market definitions (10 markets, columns, units)
catalog.data_availability -- per-table data range, row count, last update, freshness
catalog.nemweb_collections -- NEMWeb collection → MMS table mappings
catalog.schema_versions -- observed schema versions with approval status
```

An AI agent's first query is:
```sql
SELECT table_name, description, update_frequency, key_columns
FROM catalog.tables
WHERE package = 'DISPATCH' AND visibility = 'Public'
```

Then for column details:
```sql
SELECT column_name, data_type, description, units, is_primary_key, valid_values, gotcha_notes
FROM catalog.columns
WHERE table_name = 'DISPATCHPRICE'
```

Then for joins:
```sql
SELECT from_table, from_columns, to_table, to_columns, relationship_type, join_notes
FROM catalog.relationships
WHERE from_table = 'DISPATCHLOAD' OR to_table = 'DISPATCHLOAD'
```

---

## Phase 1: MMS Data Model Ingestion (source-aemo-metadata)

**What**: Parse AEMO's MMS Data Model HTML (82 pages, 500+ tables) into structured
catalog tables.

**The HTML structure** (from Elec*.htm files):
```
Per table:
  - Table name (e.g., DISPATCHPRICE)
  - Comment: one-line description
  - Description: confidentiality/visibility note
  - Source: update trigger/frequency
  - Volume: rows/day or month
  - Notes: additional context
  - Primary Key Columns: listed
  - Index Columns: listed
  - Column table:
    - Name, Data Type (VARCHAR2/NUMBER/DATE), Mandatory (X), Comment (description)
```

**Implementation**:

### 1a. Fetch the MMS Data Model HTML pages
- Base URL: `https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/Electricity%20Data%20Model%20Report_files/`
- Files: Elec1.htm through Elec82.htm (plus _1, _2 variants)
- Also fetch the TOC page for package→page mapping
- Store raw HTML in data/raw/aemo-metadata/

### 1b. Parse HTML into structured metadata
- Use `scraper` crate (already a dependency) to extract:
  - Package membership (from TOC structure)
  - Table name, description, source, volume, visibility, notes
  - Column definitions: name, oracle_type, mandatory, primary_key, description
  - Index columns
- Map Oracle types to ClickHouse types (VARCHAR2→String, NUMBER(15,5)→Float64, DATE→DateTime64)
- Extract units from column descriptions where present (e.g., "MW", "$/MWh", "%")
- Extract valid enum values from descriptions (e.g., "0=normal / 1=Supply Scarcity")

### 1c. Store in catalog tables
- `catalog.packages`: package_id, package_name, description, table_count
- `catalog.tables`: package_id, table_name, description, source_notes, update_frequency,
  volume_notes, visibility, primary_key_columns (Array), index_columns (Array)
- `catalog.columns`: table_name, column_name, ordinal, oracle_type, clickhouse_type,
  mandatory, is_primary_key, description, units, valid_values, gotcha_notes

### 1d. Parse supplementary pages
- **Table-to-Report Relationships**: Map NEMWeb file/report IDs to MMS tables
  - This answers: "which NEMWeb collection contains which MMS table?"
  - Store in `catalog.nemweb_collections`
- **Data Population Dates**: When each table started receiving data
  - Store as columns on `catalog.tables` (population_start_date, model_version)

### 1e. Re-parse on model version changes
- Poll the data model report page periodically (daily)
- Detect new model versions by comparing TOC hash
- Re-parse and update catalog, preserving history

---

## Phase 2: Reference Data Pipeline (source-nemweb extensions)

**What**: Auto-maintain live reference/lookup tables from registration data.

### 2a. Registration data collections
Add new NEMWeb collections to source-nemweb for reference data that updates slowly:

- `Next_Day_Dispatch` → contains DUDETAILSUMMARY updates
- `MMSDataModelReport` → already handled by phase 1

Or better: pull from the MMSDM DVD monthly archives for comprehensive registration snapshots.

Key registration tables to materialize:
- **DUDETAILSUMMARY**: DUID → STATIONID, REGIONID, DISPATCHTYPE, SCHEDULE_TYPE, capacity
- **STATION**: STATIONID → station name, owner, state
- **STATIONOWNER**: station ownership
- **INTERCONNECTOR**: interconnector definitions with from/to regions
- **GENUNITS**: generator unit details
- **PARTICIPANT**: participant (company) details
- **BIDTYPES**: FCAS market definitions

### 2b. Fuel type classification
Auto-classify DUIDs by fuel type using multiple signals:
1. **DISPATCHTYPE = BIDIRECTIONAL** → Battery
2. **Present in INTERMITTENT_GEN_SCADA** → Renewable (Solar/Wind)
3. **DUID naming conventions** (WF=Wind Farm, SF=Solar Farm, PV=Photovoltaic)
4. **Known hydro stations** (small list, rarely changes)
5. **Remaining scheduled** → Fossil

Store in `catalog.reference_duids`:
```sql
CREATE TABLE catalog.reference_duids (
    duid String,
    station_id String,
    station_name String,
    region_id String,
    dispatch_type String,    -- GENERATOR, LOAD, BIDIRECTIONAL
    schedule_type String,    -- SCHEDULED, SEMI-SCHEDULED, NON-SCHEDULED
    fuel_type String,        -- Solar, Wind, Battery, Hydro, Gas, Coal, Diesel, Other
    technology String,       -- CCGT, OCGT, Steam, PV Fixed, PV Tracking, Onshore Wind, etc.
    max_capacity Float64,
    participant_id String,
    participant_name String,
    connection_point String,
    is_active UInt8,
    start_date DateTime64(3),
    end_date DateTime64(3),
    last_updated DateTime64(3)
) ENGINE = ReplacingMergeTree(last_updated)
ORDER BY duid
```

### 2c. Interconnector reference
```sql
CREATE TABLE catalog.reference_interconnectors (
    interconnector_id String,  -- NSW1-QLD1, V-SA, T-V-MNSP1, etc.
    common_name String,        -- QNI, Heywood, Basslink, etc.
    from_region String,        -- NSW1
    to_region String,          -- QLD1
    is_mnsp UInt8,             -- 0=regulated, 1=merchant
    capacity_forward Float64,
    capacity_reverse Float64,
    notes String,
    last_updated DateTime64(3)
) ENGINE = ReplacingMergeTree(last_updated)
ORDER BY interconnector_id
```

### 2d. FCAS market reference
```sql
CREATE TABLE catalog.reference_fcas_markets (
    market_id String,              -- RAISE6SEC, LOWER60SEC, RAISEREG, etc.
    display_name String,           -- Raise 6 Second
    direction String,              -- RAISE or LOWER
    response_time String,          -- 6SEC, 60SEC, 5MIN, REG, 1SEC
    price_column String,           -- RAISE6SECRRP (in dispatch__price)
    price_table String,            -- dispatch__price
    volume_column String,          -- RAISE6SECLOCALDISPATCH (in dispatch__regionsum)
    volume_table String,           -- dispatch__regionsum
    enablement_column String,      -- RAISE6SECACTUALAVAILABILITY (in dispatch__unit_solution)
    enablement_table String,       -- dispatch__unit_solution
    units_price String,            -- $/MW/hr
    units_volume String,           -- MW
    notes String
) ENGINE = MergeTree()
ORDER BY market_id
```

---

## Phase 3: Gotchas, Concepts & Relationships

**What**: Encode the "tribal knowledge" that nemexplorer puts in rules.md as
queryable catalog data.

### 3a. Gotchas table
```sql
CREATE TABLE catalog.gotchas (
    id String,
    table_name String,
    column_name Nullable(String),
    severity String,               -- CRITICAL, WARNING, INFO
    category String,               -- SENTINEL_VALUE, MISLEADING_NAME, JOIN_TRAP, MISSING_DATA
    title String,
    description String,
    correct_approach String,
    wrong_approach String,
    source String                  -- 'mms_data_model', 'operational_experience', 'nemexplorer_rules'
) ENGINE = MergeTree()
ORDER BY (table_name, id)
```

Seed with known gotchas from nemexplorer's rules.md:
- DEMANDFORECAST is NOT demand forecast (it's a small adjustment value)
- END_DATE uses 1900-01-01 sentinel (never NULL)
- demand__historic has daily granularity (all timestamps at 14:00)
- DISPATCHSUBTYPE is mostly empty (don't use for fuel type)
- SCADA_TYPE valid values are LOCL and ELAV only (not GENERATION)
- dispatch__unit_solution has NO REGIONID column
- dispatch__regionsum has NO *RRP columns
- FCAS payment uses ACTUALAVAILABILITY not dispatch targets

### 3b. Concepts table (natural language → table/column routing)
```sql
CREATE TABLE catalog.concepts (
    concept String,                -- 'spot_price', 'demand', 'generation_by_fuel_type'
    natural_language_triggers Array(String),  -- ['spot price', 'RRP', 'electricity price', 'energy price']
    primary_table String,          -- 'dispatch__price'
    primary_column Nullable(String), -- 'RRP'
    alternative_table Nullable(String), -- 'trading__price' (for 30-min resolution)
    join_tables Array(String),     -- ['reference_duids'] (if fuel type needed)
    join_keys Array(String),       -- ['DUID']
    resolution String,             -- '5min', '30min', 'daily'
    units String,                  -- '$/MWh', 'MW', 'MWh'
    notes String,
    formula Nullable(String)       -- SQL formula if needed (e.g., revenue calculation)
) ENGINE = MergeTree()
ORDER BY concept
```

Seed with nemexplorer's routing rules:
- spot_price → dispatch__price.RRP (5min) or trading__price.RRP (30min)
- demand → dispatch__regionsum.TOTALDEMAND (5min), demand__historic.DEMAND (daily)
- generation → dispatch__unit_solution.TOTALCLEARED
- fuel_type_generation → dispatch__unit_solution JOIN reference_duids
- fcas_price → dispatch__price (all *RRP columns)
- fcas_volume → dispatch__regionsum (*LOCALDISPATCH columns)
- revenue → dispatch__unit_solution JOIN dispatch__price (with formula)
- bids → bids__biddayoffer, bids__bidofferperiod

### 3c. Relationships table
Auto-derive from MMS Data Model primary keys + known join patterns:
```sql
CREATE TABLE catalog.relationships (
    from_table String,
    from_columns Array(String),
    to_table String,
    to_columns Array(String),
    relationship_type String,     -- 'primary_key', 'foreign_key', 'temporal_join', 'dimensional_join'
    join_type String,             -- 'INNER', 'LEFT'
    cardinality String,           -- '1:1', '1:N', 'N:N'
    notes String,
    is_safe UInt8                 -- 1=won't cause row multiplication, 0=needs care
) ENGINE = MergeTree()
ORDER BY (from_table, to_table)
```

---

## Phase 4: Data Availability & Freshness

**What**: Track what data is actually loaded and current.

### 4a. Auto-populate from observed data
After each parse/load cycle, update:
```sql
CREATE TABLE catalog.data_availability (
    table_name String,
    view_name String,              -- nemwebviews.dispatch__price
    min_timestamp DateTime64(3),
    max_timestamp DateTime64(3),
    row_count UInt64,
    distinct_dates UInt32,
    last_loaded_at DateTime64(3),
    staleness_seconds UInt64,      -- now() - max_timestamp
    expected_frequency_seconds UInt32,  -- 300 for 5min, 1800 for 30min
    is_stale UInt8,                -- staleness > 2 * expected_frequency
    partition_count UInt32,
    total_bytes UInt64
) ENGINE = ReplacingMergeTree(last_loaded_at)
ORDER BY table_name
```

### 4b. Schema version tracking
Link observed schemas to MMS Data Model versions:
```sql
CREATE TABLE catalog.schema_versions (
    schema_id String,
    logical_table String,          -- TRADINGIS/TRADING/PRICE/3
    mms_table_name Nullable(String), -- TRADINGPRICE (matched from MMS Data Model)
    mms_package Nullable(String),  -- TRADING_DATA
    report_version String,
    model_version Nullable(String),
    header_hash String,
    approval_status String,
    first_seen_at DateTime64(3),
    last_seen_at DateTime64(3),
    column_count UInt32,
    columns_matching_mms UInt32,   -- how many columns match MMS Data Model
    columns_extra UInt32,          -- columns in data but not in MMS model
    columns_missing UInt32         -- columns in MMS model but not in data
) ENGINE = ReplacingMergeTree(last_seen_at)
ORDER BY schema_id
```

---

## Phase 5: NEMWeb Collection Expansion

**What**: Expand beyond tradingis/dispatchis to cover all critical NEMWeb collections.

### Priority tiers:

**Tier 1 — Core dispatch & pricing (currently implemented)**:
- DispatchIS_Reports (5-min prices, regional summaries, unit dispatch)
- TradingIS_Reports (trading interval prices)

**Tier 2 — Essential for AI understanding**:
- Dispatch_SCADA (unit-level SCADA MW)
- Next_Day_Dispatch (DUDETAILSUMMARY, registration data)
- Public_Prices (simplified price feed)
- Next_Day_Actual_Gen (generation by DUID)
- Rooftop_PV/ACTUAL (distributed solar)
- Network (outages, ratings)

**Tier 3 — Forecasting & planning**:
- P5_Reports (5-min pre-dispatch)
- PredispatchIS_Reports (30-min pre-dispatch)
- PDPASA (pre-dispatch reliability)
- Short_Term_PASA_Reports
- SEVENDAYOUTLOOK_FULL

**Tier 4 — Market mechanics**:
- Yesterdays_Bids_Reports (bidding behavior)
- Next_Day_Offer_Energy (energy offers)
- Settlements (financial settlement)
- Marginal_Loss_Factors

**Tier 5 — Specialist**:
- Market_Notice (market events)
- CDEII (carbon intensity)
- GBB (gas bulletin board)
- Causer_Pays (FCAS cost allocation)

### For each collection:
1. Already handled by the 108 families in source-nemweb
2. Need: registration in catalog.nemweb_collections linking collection→MMS tables
3. Need: observed schemas matched to MMS Data Model definitions

---

## Phase 6: DVD Backfill (source-aemo-dvd)

**What**: Implement historical data loading from MMSDM monthly archives.

- URL pattern: `https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{month}/`
- Each monthly archive: 225+ individual table CSVs
- Same CID CSV format as NEMWeb (I/D records)
- Includes DOCUMENTATION/ with data model docs per version

Key for AI: DVD provides the most complete snapshot of ALL tables including
registration, configuration, and historical data not available via NEMWeb current.

---

## Implementation Order

### Sprint 1: MMS Data Model Parser (Phase 1)
- [ ] Implement HTML fetcher for Elec*.htm pages
- [ ] Build scraper to extract table/column definitions
- [ ] Create catalog ClickHouse tables
- [ ] Load parsed metadata into catalog
- [ ] Add TOC parser for package→table mapping
- **Result**: `catalog.packages`, `catalog.tables`, `catalog.columns` populated

### Sprint 2: Gotchas & Concepts (Phase 3)
- [ ] Seed gotchas from nemexplorer rules.md
- [ ] Seed concepts from nemexplorer routing rules
- [ ] Build relationships from MMS primary keys
- [ ] Add FCAS market reference data
- **Result**: `catalog.gotchas`, `catalog.concepts`, `catalog.relationships`, `catalog.reference_fcas_markets`

### Sprint 3: Reference Data (Phase 2)
- [ ] Extract registration data from NEMWeb current collections
- [ ] Build fuel type classifier
- [ ] Populate DUID, interconnector, region reference tables
- **Result**: `catalog.reference_duids`, `catalog.reference_interconnectors`, `catalog.reference_regions`

### Sprint 4: Collection Mapping & Availability (Phases 4-5)
- [ ] Map NEMWeb collections to MMS tables (from report relationships page)
- [ ] Add data availability tracking after each parse cycle
- [ ] Match observed schemas to MMS Data Model definitions
- **Result**: `catalog.nemweb_collections`, `catalog.data_availability`, `catalog.schema_versions`

### Sprint 5: DVD Backfill (Phase 6)
- [ ] Implement DVD archive discovery and fetch
- [ ] Parse monthly archives (same CID format as NEMWeb)
- [ ] Bulk-load historical data
- **Result**: Historical depth for all tables back to 2009

---

## Validation: Can an AI agent work without rules.md?

For each rule in nemexplorer's rules.md, verify the catalog provides equivalent info:

| nemexplorer rule | catalog query |
|---|---|
| "spot price → dispatch__price" | `SELECT * FROM catalog.concepts WHERE 'spot price' IN natural_language_triggers` |
| "REGIONID values: NSW1, QLD1..." | `SELECT DISTINCT region_id FROM catalog.reference_regions` |
| "FCAS prices in dispatch__price not regionsum" | `SELECT * FROM catalog.gotchas WHERE table_name = 'DISPATCHREGIONSUM' AND category = 'MISSING_DATA'` |
| "DEMANDFORECAST is not demand" | `SELECT * FROM catalog.gotchas WHERE column_name = 'DEMANDFORECAST'` |
| "END_DATE uses 1900-01-01 sentinel" | `SELECT * FROM catalog.gotchas WHERE column_name = 'END_DATE'` |
| "SCADA_TYPE valid values" | `SELECT valid_values FROM catalog.columns WHERE column_name = 'SCADA_TYPE'` |
| "Revenue = MW × price × 5/60" | `SELECT formula FROM catalog.concepts WHERE concept = 'revenue'` |
| "HPR1 is active Hornsdale DUID" | `SELECT * FROM catalog.reference_duids WHERE station_name LIKE '%Hornsdale%'` |
| "T-V-MNSP1 = Basslink" | `SELECT * FROM catalog.reference_interconnectors WHERE common_name = 'Basslink'` |
| "No REGIONID in dispatch__unit_solution" | `SELECT * FROM catalog.gotchas WHERE table_name LIKE '%unit_solution%' AND column_name = 'REGIONID'` |

Every rule maps to a catalog query. The AI agent bootstraps by reading catalog tables,
not by having domain knowledge injected into its prompt.

---

## Future: Auto-Generated AI Context

Once the catalog is populated, the historian can generate per-session AI context:
1. Query `catalog.tables` for available data
2. Query `catalog.columns` for relevant columns
3. Query `catalog.gotchas` for applicable warnings
4. Query `catalog.concepts` for routing
5. Query `catalog.data_availability` for freshness

This replaces nemexplorer's static rules.md with dynamic, always-current metadata.
The context is generated per-query, not maintained manually.
