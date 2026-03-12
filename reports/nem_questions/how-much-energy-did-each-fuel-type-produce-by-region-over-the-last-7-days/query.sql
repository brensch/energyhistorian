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
    coalesce(u.REGIONID, 'UNKNOWN') AS REGIONID,
    coalesce(u.FUEL_TYPE, 'UNKNOWN') AS FUEL_TYPE,
    round(sum(g.MWH_READING), 2) AS energy_mwh
FROM actual_gen g
LEFT JOIN unit_dim u ON u.DUID = g.DUID
WHERE g.INTERVAL_DATETIME >= now() - INTERVAL 7 DAY
GROUP BY REGIONID, FUEL_TYPE
ORDER BY REGIONID, energy_mwh DESC