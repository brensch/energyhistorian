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

generator_totals AS (
    SELECT
        g.DUID,
        any(u.REGIONID) AS REGIONID,
        any(u.FUEL_TYPE) AS FUEL_TYPE,
        any(u.REGISTEREDCAPACITY_MW) AS registered_capacity_mw,
        sum(g.MWH_READING) AS energy_mwh,
        dateDiff('hour', min(g.INTERVAL_DATETIME), max(g.INTERVAL_DATETIME)) + 1 AS span_hours
    FROM actual_gen g
    INNER JOIN unit_dim u ON u.DUID = g.DUID
    WHERE g.INTERVAL_DATETIME >= now() - INTERVAL 30 DAY
      AND coalesce(u.DISPATCHTYPE, '') = 'GENERATOR'
    GROUP BY g.DUID
)

SELECT
    DUID,
    REGIONID,
    FUEL_TYPE,
    round(energy_mwh / nullIf(registered_capacity_mw * span_hours, 0), 4) AS capacity_factor
FROM generator_totals
ORDER BY capacity_factor DESC