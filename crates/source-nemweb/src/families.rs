use anyhow::{Result, anyhow};
use ingest_core::{SourceFamily, SourceFamilyCatalogEntry};

const NEMWEB_CURRENT_DIRECTORIES: &[&str] = &[
    "Adjusted_Prices_Reports",
    "Alt_Limits",
    "Ancillary_Services_Payments",
    "ANCILLARY_SERVICES_REPORTS",
    "Auction_Units_Reports",
    "Bidmove_Complete",
    "Bidmove_Summary",
    "Billing",
    "Causer_Pays",
    "Causer_Pays_Elements",
    "Causer_Pays_Rslcpf",
    "Causer_Pays_Scada",
    "CDEII",
    "CSC_CSP_ConstraintList",
    "CSC_CSP_Settlements",
    "Daily_Reports",
    "DAILYOCD",
    "Directions_Reconciliation",
    "Dispatch_IRSR",
    "DISPATCH_NEGATIVE_RESIDUE",
    "Dispatch_Reports",
    "Dispatch_SCADA",
    "DISPATCHFCST",
    "DISPATCHIS_PRICE_REVISIONS",
    "DispatchIS_Reports",
    "Dispatchprices_PRE_AP",
    "DWGM",
    "ECGS",
    "FPP",
    "FPP_HIST_REG_PERF",
    "FPPDAILY",
    "FPPRATES",
    "FPPRUN",
    "Gas_Supply_Guarantee",
    "GBB",
    "GSH",
    "HighImpactOutages",
    "HistDemand",
    "IBEI",
    "Marginal_Loss_Factors",
    "Market_Notice",
    "MCCDispatch",
    "Medium_Term_PASA_Reports",
    "Mktsusp_Pricing",
    "MMSDataModelReport",
    "MTPASA_DUIDAvailability",
    "MTPASA_RegionAvailability",
    "Network",
    "Next_Day_Actual_Gen",
    "NEXT_DAY_AVAIL_SUBMISS_CLUSTER",
    "NEXT_DAY_AVAIL_SUBMISS_DAY",
    "Next_Day_Dispatch",
    "Next_Day_Intermittent_DS",
    "Next_Day_Intermittent_Gen_Scada",
    "NEXT_DAY_MCCDISPATCH",
    "Next_Day_Offer_Energy",
    "Next_Day_Offer_Energy_SPARSE",
    "Next_Day_Offer_FCAS",
    "Next_Day_Offer_FCAS_SPARSE",
    "Next_Day_PreDispatch",
    "Next_Day_PreDispatchD",
    "Next_Day_Trading",
    "Operational_Demand",
    "Operational_Demand_Less_SNSG",
    "P5_Reports",
    "P5MINFCST",
    "PasaSnap",
    "PD7Day",
    "PDPASA",
    "PDPASA_DUIDAvailability",
    "Predispatch_IRSR",
    "Predispatch_Reports",
    "Predispatch_Sensitivities",
    "PREDISPATCHFCST",
    "PredispatchIS_Reports",
    "Public_Prices",
    "PublishedModelDataAccess",
    "Regional_Summary_Report",
    "Reserve_Contract_Recovery",
    "ROOFTOP_PV",
    "Settlements",
    "SEVENDAYOUTLOOK_FULL",
    "SEVENDAYOUTLOOK_PEAK",
    "Short_Term_PASA_Reports",
    "SRA_Bids",
    "SRA_NSR_RECONCILIATION",
    "SRA_Offers",
    "SRA_Results",
    "SSM_ENABLEMENT_COSTS",
    "SSM_ENABLEMENT_PERIOD",
    "STPASA_DUIDAvailability",
    "STTM",
    "SupplyDemand",
    "Trading_Cumulative_Price",
    "Trading_IRSR",
    "TradingIS_Reports",
    "VicGas",
    "Vwa_Fcas_Prices",
    "WDR_CAPACITY_NO_SCADA",
    "Weekly_Bulletin",
    "Weekly_Constraint_Reports",
    "Yesterdays_Bids_Reports",
    "Yesterdays_MNSPBids_Reports",
];

pub fn source_families() -> Vec<SourceFamily> {
    NEMWEB_CURRENT_DIRECTORIES
        .iter()
        .map(|directory| SourceFamily {
            id: normalize_family_id(directory),
            description: format!("NEMweb current listing `{directory}`."),
            listing_url: format!("https://nemweb.com.au/REPORTS/CURRENT/{directory}/"),
        })
        .collect()
}

pub fn lookup_family(source_family_id: &str) -> Result<SourceFamily> {
    source_families()
        .into_iter()
        .find(|family| family.id == source_family_id)
        .ok_or_else(|| anyhow!("unknown source family `{source_family_id}`"))
}

pub fn catalog_entries() -> Vec<SourceFamilyCatalogEntry> {
    source_families()
        .into_iter()
        .map(|family| SourceFamilyCatalogEntry {
            id: family.id,
            description: family.description,
            listing_url: family.listing_url,
        })
        .collect()
}

fn normalize_family_id(value: &str) -> String {
    value.to_ascii_lowercase()
}
