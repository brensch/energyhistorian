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

quarterly AS (
    SELECT
        toStartOfQuarter(g.INTERVAL_DATETIME) AS quarter_start,
        sumIf(g.MWH_READING, lower(u.FUEL_TYPE) = 'wind') AS wind_mwh,
        sum(g.MWH_READING) AS total_mwh
    FROM actual_gen g
    INNER JOIN unit_dim u ON u.DUID = g.DUID
    GROUP BY quarter_start
)

SELECT
    quarter_start,
    round(wind_mwh, 2) AS wind_mwh,
    round(total_mwh, 2) AS total_mwh,
    round(wind_mwh / nullIf(total_mwh, 0), 4) AS wind_share
FROM quarterly
ORDER BY quarter_start