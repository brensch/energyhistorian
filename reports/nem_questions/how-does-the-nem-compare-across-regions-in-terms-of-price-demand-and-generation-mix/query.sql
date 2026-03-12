WITH

region_dispatch AS (
    SELECT
        SETTLEMENTDATE,
        REGIONID,
        avg(RRP) AS RRP,
        avg(TOTALDEMAND) AS TOTALDEMAND,
        avg(DEMANDFORECAST) AS DEMANDFORECAST,
        avg(DISPATCHABLEGENERATION) AS DISPATCHABLEGENERATION,
        avg(NETINTERCHANGE) AS NETINTERCHANGE,
        avg(AVAILABLEGENERATION) AS AVAILABLEGENERATION,
        avg(EXCESSGENERATION) AS EXCESSGENERATION,
        avg(RAISE6SECDISPATCH) AS RAISE6SECDISPATCH,
        avg(RAISE60SECDISPATCH) AS RAISE60SECDISPATCH,
        avg(RAISE5MINDISPATCH) AS RAISE5MINDISPATCH,
        avg(RAISEREGLOCALDISPATCH) AS RAISEREGLOCALDISPATCH,
        avg(LOWER6SECDISPATCH) AS LOWER6SECDISPATCH,
        avg(LOWER60SECDISPATCH) AS LOWER60SECDISPATCH,
        avg(LOWER5MINDISPATCH) AS LOWER5MINDISPATCH,
        avg(LOWERREGLOCALDISPATCH) AS LOWERREGLOCALDISPATCH
    FROM semantic.daily_region_dispatch
    GROUP BY SETTLEMENTDATE, REGIONID
)
,

actual_gen AS (
    SELECT
        INTERVAL_DATETIME,
        DUID,
        avg(MWH_READING) AS MWH_READING
    FROM semantic.actual_gen_duid
    GROUP BY INTERVAL_DATETIME, DUID
)
,

unit_dim AS (
    SELECT
        DUID,
        REGIONID,
        EFFECTIVE_PARTICIPANTID,
        PARTICIPANTID,
        STATIONID,
        STATIONNAME,
        FUEL_TYPE,
        ENERGY_SOURCE,
        REGISTEREDCAPACITY_MW,
        MAXCAPACITY_MW,
        MAXSTORAGECAPACITY_MWH,
        CO2E_EMISSIONS_FACTOR,
        IS_STORAGE,
        IS_BIDIRECTIONAL,
        DISPATCHTYPE,
        SCHEDULE_TYPE
    FROM semantic.unit_dimension
)
,

gen_mix AS (
    SELECT
        u.REGIONID,
        
CASE
    WHEN lower(FUEL_TYPE) IN (
        'solar', 'wind', 'hydro', 'battery storage', 'bagasse',
        'biomass and industrial materials', 'other biofuels',
        'primary solid biomass fuels', 'landfill biogas methane'
    ) THEN 'Renewables'
    ELSE 'Fossil and other'
END
 AS generation_group,
        sum(g.MWH_READING) AS energy_mwh
    FROM actual_gen g
    INNER JOIN unit_dim u ON u.DUID = g.DUID
    GROUP BY u.REGIONID, generation_group
)

SELECT
    d.REGIONID,
    round(avg(d.RRP), 2) AS avg_price_mwh,
    round(avg(d.TOTALDEMAND), 2) AS avg_demand_mw,
    round(maxIf(g.energy_mwh, g.generation_group = 'Renewables') / nullIf(sum(g.energy_mwh), 0), 4) AS renewable_share
FROM region_dispatch d
LEFT JOIN gen_mix g ON g.REGIONID = d.REGIONID
GROUP BY d.REGIONID
ORDER BY avg_price_mwh DESC