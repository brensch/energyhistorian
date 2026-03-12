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

gas_units AS (
    SELECT
        u.DUID,
        u.REGIONID,
        u.REGISTEREDCAPACITY_MW,
        sum(g.MWH_READING) AS energy_mwh,
        dateDiff('hour', min(g.INTERVAL_DATETIME), max(g.INTERVAL_DATETIME)) + 1 AS span_hours
    FROM unit_dim u
    LEFT JOIN actual_gen g ON g.DUID = u.DUID
    WHERE lower(u.FUEL_TYPE) = 'natural gas (pipeline)'
    GROUP BY u.DUID, u.REGIONID, u.REGISTEREDCAPACITY_MW
)

SELECT
    REGIONID,
    round(sumIf(REGISTEREDCAPACITY_MW, energy_mwh / nullIf(REGISTEREDCAPACITY_MW * span_hours, 0) < 0.1), 2) AS rarely_dispatched_capacity_mw
FROM gas_units
GROUP BY REGIONID
ORDER BY rarely_dispatched_capacity_mw DESC