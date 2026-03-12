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

price_dispatch AS (
    SELECT
        SETTLEMENTDATE,
        REGIONID,
        avg(RRP) AS RRP,
        avg(RAISE6SECRRP) AS RAISE6SECRRP,
        avg(RAISE60SECRRP) AS RAISE60SECRRP,
        avg(RAISE5MINRRP) AS RAISE5MINRRP,
        avg(RAISEREGRRP) AS RAISEREGRRP,
        avg(RAISE1SECRRP) AS RAISE1SECRRP,
        avg(LOWER6SECRRP) AS LOWER6SECRRP,
        avg(LOWER60SECRRP) AS LOWER60SECRRP,
        avg(LOWER5MINRRP) AS LOWER5MINRRP,
        avg(LOWERREGRRP) AS LOWERREGRRP,
        avg(LOWER1SECRRP) AS LOWER1SECRRP
    FROM semantic.dispatch_price
    GROUP BY SETTLEMENTDATE, REGIONID
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

generator_metrics AS (
    SELECT
        g.DUID AS DUID,
        any(u.FUEL_TYPE) AS FUEL_TYPE,
        any(u.REGISTEREDCAPACITY_MW) AS registered_capacity_mw,
        sum(g.MWH_READING) AS energy_mwh,
        sum(g.MWH_READING * p.RRP) AS revenue_proxy,
        dateDiff('hour', min(g.INTERVAL_DATETIME), max(g.INTERVAL_DATETIME)) + 1 AS span_hours
    FROM actual_gen g
    INNER JOIN unit_dim u ON u.DUID = g.DUID
    LEFT JOIN price_dispatch p ON p.SETTLEMENTDATE = g.INTERVAL_DATETIME AND p.REGIONID = u.REGIONID
    WHERE coalesce(u.DISPATCHTYPE, '') = 'GENERATOR'
    GROUP BY g.DUID
)

SELECT
    DUID,
    FUEL_TYPE,
    round(revenue_proxy, 2) AS revenue_proxy,
    round(energy_mwh / nullIf(registered_capacity_mw * span_hours, 0), 4) AS capacity_factor
FROM generator_metrics
ORDER BY revenue_proxy DESC
LIMIT 100