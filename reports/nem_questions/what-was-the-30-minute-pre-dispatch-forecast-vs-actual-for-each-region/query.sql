WITH

predispatch_price AS (
    SELECT
        DATETIME,
        REGIONID,
        avg(RRP) AS RRP
    FROM semantic.predispatch_region_prices
    GROUP BY DATETIME, REGIONID
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

SELECT
    p.DATETIME,
    p.REGIONID,
    round(p.RRP, 2) AS predispatch_rrp,
    round(a.RRP, 2) AS actual_rrp,
    round(p.RRP - a.RRP, 2) AS error_rrp
FROM predispatch_price p
LEFT JOIN price_dispatch a ON a.SETTLEMENTDATE = p.DATETIME AND a.REGIONID = p.REGIONID
ORDER BY p.DATETIME, p.REGIONID