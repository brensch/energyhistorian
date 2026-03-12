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
    DUID,
    any(REGIONID) AS REGIONID,
    any(FUEL_TYPE) AS FUEL_TYPE,
    min(delta_mwh) AS largest_interval_drop_mwh
FROM (
    SELECT
        g.INTERVAL_DATETIME,
        g.DUID,
        u.REGIONID,
        u.FUEL_TYPE,
        g.MWH_READING - lagInFrame(g.MWH_READING) OVER (
            PARTITION BY g.DUID ORDER BY g.INTERVAL_DATETIME
        ) AS delta_mwh
    FROM actual_gen g
    INNER JOIN unit_dim u ON u.DUID = g.DUID
)
GROUP BY DUID
ORDER BY largest_interval_drop_mwh ASC
LIMIT 25