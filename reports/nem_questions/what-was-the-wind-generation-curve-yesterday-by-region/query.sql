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
    toStartOfHour(g.INTERVAL_DATETIME) AS hour_start,
    u.REGIONID,
    round(sum(g.MWH_READING), 2) AS wind_mwh
FROM actual_gen g
INNER JOIN unit_dim u ON u.DUID = g.DUID
WHERE toDate(g.INTERVAL_DATETIME) = yesterday()
  AND lower(u.FUEL_TYPE) = 'wind'
GROUP BY hour_start, u.REGIONID
ORDER BY hour_start, u.REGIONID