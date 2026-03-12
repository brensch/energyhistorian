WITH

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

paired AS (
    SELECT
        toDate(SETTLEMENTDATE) AS trading_day,
        avgIf(RRP, REGIONID = 'NSW1') AS nsw_rrp,
        avgIf(RRP, REGIONID = 'VIC1') AS vic_rrp
    FROM price_dispatch
    WHERE SETTLEMENTDATE >= now() - INTERVAL 7 DAY
    GROUP BY trading_day
)

SELECT
    trading_day,
    round(nsw_rrp - vic_rrp, 2) AS nsw_vic_spread_mwh
FROM paired
ORDER BY trading_day