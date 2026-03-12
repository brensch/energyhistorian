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

hourly_counts AS (
    SELECT
        toStartOfHour(g.INTERVAL_DATETIME) AS hour_start,
        countDistinctIf(g.DUID, g.MWH_READING > 0 AND coalesce(u.DISPATCHTYPE, '') = 'GENERATOR') AS generators_online
    FROM actual_gen g
    LEFT JOIN unit_dim u ON u.DUID = g.DUID
    GROUP BY hour_start
)

SELECT
    toHour(hour_start) AS hour_of_day,
    round(avg(generators_online), 2) AS avg_generators_online
FROM hourly_counts
GROUP BY hour_of_day
ORDER BY hour_of_day