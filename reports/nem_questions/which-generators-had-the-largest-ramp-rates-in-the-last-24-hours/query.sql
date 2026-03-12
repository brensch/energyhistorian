WITH

unit_dispatch AS (
    SELECT
        SETTLEMENTDATE,
        DUID,
        avg(TOTALCLEARED) AS TOTALCLEARED,
        avg(INITIALMW) AS INITIALMW,
        avg(AVAILABILITY) AS AVAILABILITY,
        avg(RAMPUPRATE) AS RAMPUPRATE,
        avg(RAMPDOWNRATE) AS RAMPDOWNRATE,
        avg(RAISE6SEC + RAISE60SEC + RAISE5MIN + RAISEREG) AS FCAS_RAISE_TOTAL,
        avg(LOWER6SEC + LOWER60SEC + LOWER5MIN + LOWERREG) AS FCAS_LOWER_TOTAL
    FROM semantic.daily_unit_dispatch
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
    DUID,
    any(REGIONID) AS REGIONID,
    any(FUEL_TYPE) AS FUEL_TYPE,
    max(abs(delta_mw_per_interval) * 12.0) AS max_ramp_rate_mw_per_hour
FROM (
    SELECT
        d.SETTLEMENTDATE,
        d.DUID,
        u.REGIONID,
        u.FUEL_TYPE,
        d.TOTALCLEARED - lagInFrame(d.TOTALCLEARED) OVER (
            PARTITION BY d.DUID ORDER BY d.SETTLEMENTDATE
        ) AS delta_mw_per_interval
    FROM unit_dispatch d
    INNER JOIN unit_dim u ON u.DUID = d.DUID
    WHERE d.SETTLEMENTDATE >= now() - INTERVAL 24 HOUR
      AND coalesce(u.DISPATCHTYPE, '') = 'GENERATOR'
)
GROUP BY DUID
ORDER BY max_ramp_rate_mw_per_hour DESC
LIMIT 20