// ── MMSDM Table Definitions ──────────────────────────────────────────────
//
// This module defines which tables from the AEMO MMSDM monthly archive we
// capture.  MMSDM publishes ~200 tables per month; we pick the ones that are
// most useful for energy-market analysis and reference-data enrichment.
//
// Each table belongs to a *category* — a human-readable grouping that has no
// effect on ingestion but helps when browsing the list.
//
// To add a new table: push an entry into the appropriate category's block in
// `build_table_list()`.  The table name must match the name that appears in
// the zip filename:  PUBLIC_ARCHIVE#<TABLE_NAME>#FILE…#….zip
//
// The parser (source-nemweb::parse) handles the CSV inside the zip
// generically, so no per-table parsing code is needed.

/// A single MMSDM table we want to capture from the monthly archive.
pub struct MmsdmTable {
    /// Exact table name as it appears in the zip filename (uppercase).
    pub name: &'static str,
    /// Human-readable category for grouping (e.g. "Registration", "Dispatch").
    pub category: &'static str,
    /// Short description of what this table contains.
    pub description: &'static str,
}

/// Returns the full list of MMSDM tables we ingest.
///
/// Tables are grouped by category.  Within each category they are listed
/// alphabetically.  This ordering is cosmetic — discovery matches by name
/// regardless of order.
pub fn include_tables() -> &'static [MmsdmTable] {
    &TABLES
}

/// Returns just the table names as a slice-of-str, for quick membership
/// checks during discovery.
pub fn include_table_names() -> Vec<&'static str> {
    TABLES.iter().map(|t| t.name).collect()
}

// ---------------------------------------------------------------------------
// Table list
//
// Categories (roughly matching AEMO's own grouping):
//
//   Registration   – who/what is in the market (DUIDs, stations, owners, …)
//   Dispatch       – 5-minute actual dispatch outcomes
//   Pricing        – regional and local settlement prices
//   Bidding        – generator/load bid data
//   Interconnector – inter-region flow and constraint data
//   Demand         – operational demand (actual + forecast)
//   Constraints    – generic constraint definitions and invocations
//   Intermittent   – wind/solar forecasts and SCADA
//   Rooftop PV     – behind-the-meter solar estimates
//   MTPASA         – medium-term reliability assessments
//   Region         – NEM region reference data
//   Network        – transmission network topology and ratings
//   Market         – fee schedules, price thresholds, suspension data
// ---------------------------------------------------------------------------

static TABLES: [MmsdmTable; 73] = [
    // ── Registration ────────────────────────────────────────────────────
    //
    // These tables describe the physical and commercial attributes of
    // generating units (DUIDs), stations, and their participants.  They
    // are slowly-changing-dimension style: each row has an effective date
    // or LASTCHANGED timestamp, so multiple versions exist per entity.
    //
    // Together they feed the `unit_dimension` semantic view, which is the
    // single "current state" lookup table for every DUID in the NEM.
    MmsdmTable {
        name: "DUALLOC",
        category: "Registration",
        description: "Maps each DUID to its generating-set (GENSETID). \
            A DUID can change GENSET over time so this is effective-dated.",
    },
    MmsdmTable {
        name: "DUDETAIL",
        category: "Registration",
        description: "Detailed DUID registration attributes including dispatch type \
            and maximum storage capacity. Effective-dated per DUID.",
    },
    MmsdmTable {
        name: "DUDETAILSUMMARY",
        category: "Registration",
        description: "Summary DUID registration record: region, participant, station, \
            connection point, loss factors, ramp rates. The primary source for \
            the unit_dimension view.",
    },
    MmsdmTable {
        name: "GENUNITS",
        category: "Registration",
        description: "Generating-set physical attributes: fuel type (CO2E_ENERGY_SOURCE), \
            registered/max capacity, and CO2 emissions factor.",
    },
    MmsdmTable {
        name: "GENUNITS_UNIT",
        category: "Registration",
        description: "Additional per-unit detail within a generating set. \
            Links GENSETID to individual physical units.",
    },
    MmsdmTable {
        name: "STADUALLOC",
        category: "Registration",
        description: "Station-to-DUID allocation. Maps which DUIDs belong to \
            which station (stationid).",
    },
    MmsdmTable {
        name: "STATION",
        category: "Registration",
        description: "Station reference data: name, state/territory. \
            Historised by LASTCHANGED.",
    },
    MmsdmTable {
        name: "STATIONOWNER",
        category: "Registration",
        description: "Station ownership: which participant owns each station, \
            effective-dated.",
    },
    MmsdmTable {
        name: "STATIONOWNERTRK",
        category: "Registration",
        description: "Tracking/audit table for station ownership changes.",
    },
    MmsdmTable {
        name: "STATIONOPERATINGSTATUS",
        category: "Registration",
        description: "Operating status of each station (e.g. commissioned, \
            decommissioned).",
    },
    MmsdmTable {
        name: "PARTICIPANT",
        category: "Registration",
        description: "Registered market participant reference data (company names, IDs).",
    },
    MmsdmTable {
        name: "PARTICIPANTCLASS",
        category: "Registration",
        description: "Classification of participants (generator, customer, etc.).",
    },
    MmsdmTable {
        name: "PARTICIPANTCATEGORY",
        category: "Registration",
        description: "Category codes for participant types.",
    },
    MmsdmTable {
        name: "PARTICIPANTCATEGORYALLOC",
        category: "Registration",
        description: "Which categories each participant is allocated to.",
    },
    MmsdmTable {
        name: "DISPATCHABLEUNIT",
        category: "Registration",
        description: "Master list of all dispatchable units (DUIDs) in the NEM.",
    },
    MmsdmTable {
        name: "TRANSMISSIONLOSSFACTOR",
        category: "Registration",
        description: "Transmission loss factors by connection point and financial year.",
    },

    // ── Dispatch ─────────────────────────────────────────────────────────
    //
    // Actual 5-minute dispatch outcomes from NEMDE (the dispatch engine).
    // These are the definitive record of what the market did — who generated
    // how much, at what price, subject to which constraints.
    MmsdmTable {
        name: "DISPATCHLOAD",
        category: "Dispatch",
        description: "Per-DUID dispatch targets every 5 minutes: MW output, \
            FCAS enablement, ramp rates. The core unit-level dispatch table.",
    },
    MmsdmTable {
        name: "DISPATCHPRICE",
        category: "Dispatch",
        description: "Regional reference prices (energy + 8 FCAS) for each \
            5-minute dispatch interval.",
    },
    MmsdmTable {
        name: "DISPATCHREGIONSUM",
        category: "Dispatch",
        description: "Regional summary per dispatch interval: total demand, \
            generation, net interchange, surplus.",
    },
    MmsdmTable {
        name: "DISPATCHINTERCONNECTORRES",
        category: "Dispatch",
        description: "Interconnector flow results per dispatch interval: MW flow, \
            losses, limits, marginal values.",
    },
    MmsdmTable {
        name: "DISPATCHCONSTRAINT",
        category: "Dispatch",
        description: "Binding constraint results per dispatch interval: RHS, \
            marginal value, violation degree.",
    },
    MmsdmTable {
        name: "DISPATCHCASESOLUTION",
        category: "Dispatch",
        description: "Dispatch engine case-level metadata: intervention flag, \
            solution status, total objective function.",
    },
    MmsdmTable {
        name: "DISPATCH_UNIT_SCADA",
        category: "Dispatch",
        description: "Actual SCADA MW readings per DUID per dispatch interval. \
            Independent of dispatch targets — shows what units actually produced.",
    },
    MmsdmTable {
        name: "DISPATCH_LOCAL_PRICE",
        category: "Dispatch",
        description: "Local (connection-point) prices per dispatch interval. \
            Differs from regional price by loss factor adjustments.",
    },

    // ── Pricing ──────────────────────────────────────────────────────────
    //
    // Trading (30-min settlement) and daily prices.
    MmsdmTable {
        name: "TRADINGPRICE",
        category: "Pricing",
        description: "30-minute trading interval prices per region. \
            Settlement prices used for financial calculations.",
    },
    MmsdmTable {
        name: "TRADINGINTERCONNECT",
        category: "Pricing",
        description: "30-minute trading interval interconnector flows.",
    },
    MmsdmTable {
        name: "MARKET_PRICE_THRESHOLDS",
        category: "Pricing",
        description: "Market price cap (MPC), cumulative price threshold (CPT), \
            and administered price cap values by effective date.",
    },

    // ── Bidding ──────────────────────────────────────────────────────────
    //
    // Generator and load bids submitted to the market.  Day-offer is the
    // daily bid stack; period-offer is the per-interval bid detail.
    MmsdmTable {
        name: "BIDDAYOFFER",
        category: "Bidding",
        description: "Daily bid price/quantity pairs per DUID and bid type \
            (energy, FCAS). Up to 10 price bands per bid.",
    },
    MmsdmTable {
        name: "BIDDAYOFFER_D",
        category: "Bidding",
        description: "Processed/default version of daily bid offers.",
    },
    MmsdmTable {
        name: "BIDOFFERPERIOD",
        category: "Bidding",
        description: "Per-interval bid availability and band quantities. \
            Shows how much capacity was offered at each price band.",
    },
    MmsdmTable {
        name: "BIDPEROFFER_D",
        category: "Bidding",
        description: "Processed/default version of per-interval bid offers.",
    },
    MmsdmTable {
        name: "BIDDUIDDETAILS",
        category: "Bidding",
        description: "DUID-level bid configuration: which bid types are \
            applicable, price band count.",
    },
    MmsdmTable {
        name: "BIDTYPES",
        category: "Bidding",
        description: "Reference table of bid type codes (ENERGY, RAISEREG, etc.).",
    },
    MmsdmTable {
        name: "MNSP_DAYOFFER",
        category: "Bidding",
        description: "Daily bid offers for Market Network Service Providers \
            (interconnectors that bid like generators).",
    },
    MmsdmTable {
        name: "MNSP_BIDOFFERPERIOD",
        category: "Bidding",
        description: "Per-interval MNSP bid detail.",
    },

    // ── Interconnectors ──────────────────────────────────────────────────
    //
    // Reference and constraint data for the inter-regional interconnectors
    // (e.g. VIC-NSW, QLD-NSW, Basslink).
    MmsdmTable {
        name: "INTERCONNECTOR",
        category: "Interconnector",
        description: "Interconnector reference data: from/to regions, \
            description, classification.",
    },
    MmsdmTable {
        name: "INTERCONNECTORCONSTRAINT",
        category: "Interconnector",
        description: "Interconnector limit equations: how constraints map \
            to interconnector transfer limits.",
    },
    MmsdmTable {
        name: "MNSP_INTERCONNECTOR",
        category: "Interconnector",
        description: "MNSP (market network service provider) interconnector \
            configuration. Basslink and similar merchant links.",
    },
    MmsdmTable {
        name: "DISPATCH_INTERCONNECTION",
        category: "Interconnector",
        description: "Interconnector dispatch results with detailed flow \
            and loss calculations.",
    },
    MmsdmTable {
        name: "METERDATA_INTERCONNECTOR",
        category: "Interconnector",
        description: "Metered interconnector flows (actual, vs dispatch targets).",
    },

    // ── Demand ───────────────────────────────────────────────────────────
    //
    // Operational demand data — what consumers are actually drawing from
    // the grid, and forecasts of what they will draw.
    MmsdmTable {
        name: "DEMANDOPERATIONALACTUAL",
        category: "Demand",
        description: "Actual operational demand per region per 5-minute interval. \
            This is demand net of rooftop PV and small non-scheduled generation.",
    },
    MmsdmTable {
        name: "DEMANDOPERATIONALFORECAST",
        category: "Demand",
        description: "Operational demand forecasts per region.",
    },
    MmsdmTable {
        name: "PERDEMAND",
        category: "Demand",
        description: "Historical period demand data by region.",
    },

    // ── Constraints ──────────────────────────────────────────────────────
    //
    // Generic constraint definitions and their invocation history.
    // Constraints are the rules NEMDE uses to enforce network limits,
    // system security, and market rules.
    MmsdmTable {
        name: "GENCONDATA",
        category: "Constraint",
        description: "Generic constraint header data: constraint ID, \
            description, type, effective dates.",
    },
    MmsdmTable {
        name: "GENCONSET",
        category: "Constraint",
        description: "Constraint set definitions: groups of related constraints.",
    },
    MmsdmTable {
        name: "GENCONSETINVOKE",
        category: "Constraint",
        description: "Constraint set invocation log: when each constraint set \
            was activated/deactivated in the market.",
    },
    MmsdmTable {
        name: "GENERICCONSTRAINTRHS",
        category: "Constraint",
        description: "Right-hand-side values for generic constraints. \
            The actual MW limits that constraints enforce.",
    },
    MmsdmTable {
        name: "SPDCONNECTIONPOINTCONSTRAINT",
        category: "Constraint",
        description: "Connection-point factors in SPD constraint equations. \
            How each DUID participates in each constraint.",
    },
    MmsdmTable {
        name: "SPDINTERCONNECTORCONSTRAINT",
        category: "Constraint",
        description: "Interconnector factors in SPD constraint equations.",
    },
    MmsdmTable {
        name: "SPDREGIONCONSTRAINT",
        category: "Constraint",
        description: "Region-level factors in SPD constraint equations.",
    },

    // ── Intermittent Generation ──────────────────────────────────────────
    //
    // Wind and solar forecast data, cluster availability, and SCADA readings
    // for semi-scheduled generators.
    MmsdmTable {
        name: "INTERMITTENT_DS_PRED",
        category: "Intermittent",
        description: "Dispatch-interval forecasts for intermittent generators \
            (wind/solar). Used by NEMDE to set available capacity.",
    },
    MmsdmTable {
        name: "INTERMITTENT_DS_RUN",
        category: "Intermittent",
        description: "Intermittent forecast run metadata: model version, run time.",
    },
    MmsdmTable {
        name: "INTERMITTENT_GEN_SCADA",
        category: "Intermittent",
        description: "SCADA readings for intermittent generators. \
            Actual MW output at each dispatch interval.",
    },
    MmsdmTable {
        name: "INTERMITTENT_CLUSTER_AVAIL",
        category: "Intermittent",
        description: "Intermittent cluster availability per dispatch interval. \
            Shows available capacity from wind/solar clusters.",
    },
    MmsdmTable {
        name: "INTERMITTENT_CLUSTER_AVAIL_DAY",
        category: "Intermittent",
        description: "Daily intermittent cluster availability summary.",
    },
    MmsdmTable {
        name: "INTERMITTENT_GEN_LIMIT",
        category: "Intermittent",
        description: "Generation limits applied to intermittent generators \
            per dispatch interval.",
    },
    MmsdmTable {
        name: "INTERMITTENT_GEN_LIMIT_DAY",
        category: "Intermittent",
        description: "Daily generation limit summary for intermittent generators.",
    },

    // ── Rooftop PV ───────────────────────────────────────────────────────
    //
    // AEMO's estimates of behind-the-meter rooftop solar generation.
    // Critical for understanding true demand and net load.
    MmsdmTable {
        name: "ROOFTOP_PV_ACTUAL",
        category: "Rooftop PV",
        description: "Estimated actual rooftop PV generation per region \
            per 30-minute interval.",
    },
    MmsdmTable {
        name: "ROOFTOP_PV_FORECAST",
        category: "Rooftop PV",
        description: "Rooftop PV generation forecasts per region.",
    },

    // ── MTPASA ───────────────────────────────────────────────────────────
    //
    // Medium-Term Projected Assessment of System Adequacy.  Covers the
    // 2-year outlook for generation adequacy and reliability.
    MmsdmTable {
        name: "MTPASA_REGIONRESULT",
        category: "MTPASA",
        description: "MTPASA region-level results: unserved energy, \
            reserve margin, demand forecasts over the 2-year horizon.",
    },
    MmsdmTable {
        name: "MTPASA_DUIDAVAILABILITY",
        category: "MTPASA",
        description: "DUID-level availability declarations for MTPASA: \
            planned outages and available capacity.",
    },
    MmsdmTable {
        name: "MTPASA_INTERCONNECTORRESULT",
        category: "MTPASA",
        description: "MTPASA interconnector transfer capability results.",
    },

    // ── Region ───────────────────────────────────────────────────────────
    MmsdmTable {
        name: "REGION",
        category: "Region",
        description: "NEM region definitions (NSW1, VIC1, QLD1, SA1, TAS1).",
    },
    MmsdmTable {
        name: "REGIONSTANDINGDATA",
        category: "Region",
        description: "Standing data per region: RSPA demand thresholds, \
            minimum reserve levels.",
    },

    // ── Network ──────────────────────────────────────────────────────────
    //
    // Transmission network topology.  Useful for understanding where
    // constraints come from and how the physical grid connects.
    MmsdmTable {
        name: "NETWORK_EQUIPMENTDETAIL",
        category: "Network",
        description: "Transmission equipment details: line/transformer ratings, \
            voltage levels, from/to substations.",
    },
    MmsdmTable {
        name: "NETWORK_RATING",
        category: "Network",
        description: "Dynamic thermal ratings for network equipment.",
    },
    MmsdmTable {
        name: "NETWORK_STATICRATING",
        category: "Network",
        description: "Static (nameplate) thermal ratings for network equipment.",
    },
    MmsdmTable {
        name: "NETWORK_SUBSTATIONDETAIL",
        category: "Network",
        description: "Substation reference data: location, region, voltage levels.",
    },
    MmsdmTable {
        name: "NETWORK_OUTAGEDETAIL",
        category: "Network",
        description: "Planned and forced network outage details: equipment, \
            start/end times, reason codes.",
    },

    // ── Market ───────────────────────────────────────────────────────────
    MmsdmTable {
        name: "MARKETFEE",
        category: "Market",
        description: "Market fee types and rates charged to participants.",
    },
    MmsdmTable {
        name: "MARKETFEEDATA",
        category: "Market",
        description: "Market fee data: fee rates by effective date.",
    },
    MmsdmTable {
        name: "MARKETSUSPENSION",
        category: "Market",
        description: "Market suspension events: when normal pricing was \
            replaced by administered pricing.",
    },
    MmsdmTable {
        name: "MARKET_SUSPEND_SCHEDULE",
        category: "Market",
        description: "Administered price schedule during market suspensions.",
    },
];
