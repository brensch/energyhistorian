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

daily_swings AS (
    SELECT
        toDate(SETTLEMENTDATE) AS trading_day,
        REGIONID,
        max(RRP) - min(RRP) AS intraday_swing
    FROM price_dispatch
    GROUP BY trading_day, REGIONID
)

SELECT
    REGIONID,
    round(avg(intraday_swing), 2) AS avg_intraday_swing,
    round(max(intraday_swing), 2) AS max_intraday_swing
FROM daily_swings
GROUP BY REGIONID
ORDER BY avg_intraday_swing DESC