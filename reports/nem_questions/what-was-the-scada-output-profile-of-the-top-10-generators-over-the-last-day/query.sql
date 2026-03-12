WITH

unit_scada AS (
    SELECT
        SETTLEMENTDATE,
        DUID,
        avg(SCADAVALUE) AS SCADAVALUE
    FROM semantic.dispatch_unit_scada
    GROUP BY SETTLEMENTDATE, DUID
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

top_generators AS (
    SELECT DUID
    FROM (
        SELECT DUID, avg(SCADAVALUE) AS avg_scada
        FROM unit_scada
        WHERE SETTLEMENTDATE >= now() - INTERVAL 1 DAY
        GROUP BY DUID
        ORDER BY avg_scada DESC
        LIMIT 10
    )
)

SELECT
    s.SETTLEMENTDATE,
    s.DUID,
    any(u.FUEL_TYPE) AS FUEL_TYPE,
    avg(s.SCADAVALUE) AS scada_mw
FROM unit_scada s
LEFT JOIN unit_dim u ON u.DUID = s.DUID
WHERE s.SETTLEMENTDATE >= now() - INTERVAL 1 DAY
  AND s.DUID IN (SELECT DUID FROM top_generators)
GROUP BY s.SETTLEMENTDATE, s.DUID
ORDER BY s.SETTLEMENTDATE, s.DUID