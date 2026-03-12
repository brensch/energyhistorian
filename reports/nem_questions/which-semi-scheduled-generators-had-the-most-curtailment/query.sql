WITH

unit_solution AS (
    SELECT
        SETTLEMENTDATE,
        DUID,
        avg(TOTALCLEARED) AS TOTALCLEARED,
        avg(UIGF) AS UIGF
    FROM semantic.dispatch_unit_solution
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

SELECT
    s.DUID,
    any(u.REGIONID) AS REGIONID,
    any(u.FUEL_TYPE) AS FUEL_TYPE,
    round(sum(greatest(s.UIGF - s.TOTALCLEARED, 0) / 12.0), 2) AS curtailed_mwh_proxy
FROM unit_solution s
INNER JOIN unit_dim u ON u.DUID = s.DUID
WHERE coalesce(u.SCHEDULE_TYPE, '') = 'SEMI-SCHEDULED'
GROUP BY s.DUID
ORDER BY curtailed_mwh_proxy DESC
LIMIT 25