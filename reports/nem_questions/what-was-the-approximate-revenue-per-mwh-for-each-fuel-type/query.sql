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

SELECT
    any(u.FUEL_TYPE) AS FUEL_TYPE,
    round(sum(g.MWH_READING * p.RRP), 2) AS revenue_proxy,
    round(sum(g.MWH_READING), 2) AS energy_mwh,
    round(sum(g.MWH_READING * p.RRP) / nullIf(sum(g.MWH_READING), 0), 2) AS revenue_per_mwh_proxy
FROM actual_gen g
INNER JOIN unit_dim u ON u.DUID = g.DUID
LEFT JOIN price_dispatch p ON p.SETTLEMENTDATE = g.INTERVAL_DATETIME AND p.REGIONID = u.REGIONID
WHERE coalesce(u.DISPATCHTYPE, '') = 'GENERATOR'
GROUP BY u.FUEL_TYPE
ORDER BY revenue_per_mwh_proxy DESC