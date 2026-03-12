WITH

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

base AS (
    SELECT
        coalesce(u.REGIONID, 'UNKNOWN') AS REGIONID,
        
CASE
    WHEN lower(FUEL_TYPE) IN (
        'solar', 'wind', 'hydro', 'battery storage', 'bagasse',
        'biomass and industrial materials', 'other biofuels',
        'primary solid biomass fuels', 'landfill biogas methane'
    ) THEN 'Renewables'
    ELSE 'Fossil and other'
END
 AS generation_group,
        g.MWH_READING AS energy_mwh
    FROM actual_gen g
    LEFT JOIN unit_dim u ON u.DUID = g.DUID
    WHERE g.INTERVAL_DATETIME >= now() - INTERVAL 7 DAY
)

SELECT
    REGIONID,
    generation_group,
    round(energy_mwh, 2) AS energy_mwh,
    round(energy_mwh / sum(energy_mwh) OVER (PARTITION BY REGIONID), 4) AS share_of_region
FROM (
    SELECT
        REGIONID,
        generation_group,
        sum(energy_mwh) AS energy_mwh
    FROM base
    GROUP BY REGIONID, generation_group
)
ORDER BY REGIONID, share_of_region DESC