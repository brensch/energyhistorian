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

SELECT
    u.REGIONID,
    round(sum(g.MWH_READING * coalesce(u.CO2E_EMISSIONS_FACTOR, 0)) / nullIf(sum(g.MWH_READING), 0), 3) AS tco2e_per_mwh
FROM actual_gen g
INNER JOIN unit_dim u ON u.DUID = g.DUID
GROUP BY u.REGIONID
ORDER BY tco2e_per_mwh ASC
LIMIT 1