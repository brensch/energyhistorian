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
    u.FUEL_TYPE,
    round(sum(g.MWH_READING), 2) AS generation_mwh
FROM actual_gen g
INNER JOIN unit_dim u ON u.DUID = g.DUID
WHERE u.REGIONID IN ('SA1', 'NSW1')
GROUP BY u.REGIONID, u.FUEL_TYPE
ORDER BY u.REGIONID, generation_mwh DESC