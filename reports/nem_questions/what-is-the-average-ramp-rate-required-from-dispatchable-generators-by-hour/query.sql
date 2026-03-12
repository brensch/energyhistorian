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
,

hourly_ramp AS (
    SELECT
        d.SETTLEMENTDATE,
        toHour(d.SETTLEMENTDATE) AS hour_of_day,
        d.DUID,
        abs(d.TOTALCLEARED - lagInFrame(d.TOTALCLEARED) OVER (
            PARTITION BY d.DUID ORDER BY d.SETTLEMENTDATE
        )) * 12.0 AS ramp_rate_mw_per_hour
    FROM unit_dispatch d
    INNER JOIN unit_dim u ON u.DUID = d.DUID
    WHERE lower(u.FUEL_TYPE) NOT IN ('solar', 'wind')
)

SELECT
    hour_of_day,
    round(avg(ramp_rate_mw_per_hour), 2) AS avg_ramp_rate_mw_per_hour
FROM hourly_ramp
GROUP BY hour_of_day
ORDER BY hour_of_day