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
    u.DUID,
    u.REGIONID,
    u.FUEL_TYPE,
    round(avg(g.MWH_READING * 12.0), 2) AS avg_output_mw,
    round(any(u.REGISTEREDCAPACITY_MW), 2) AS registered_capacity_mw,
    round(avg(g.MWH_READING * 12.0) / nullIf(any(u.REGISTEREDCAPACITY_MW), 0), 3) AS average_capacity_factor
FROM actual_gen g
INNER JOIN unit_dim u ON u.DUID = g.DUID
WHERE lower(u.FUEL_TYPE) IN ('solar', 'wind')
GROUP BY u.DUID, u.REGIONID, u.FUEL_TYPE
ORDER BY average_capacity_factor DESC