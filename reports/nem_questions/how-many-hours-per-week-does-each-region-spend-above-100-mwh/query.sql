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

hourly_price AS (
    SELECT
        toStartOfHour(SETTLEMENTDATE) AS hour_start,
        REGIONID,
        avg(RRP) AS hourly_avg_price
    FROM price_dispatch
    GROUP BY hour_start, REGIONID
)

SELECT
    REGIONID,
    toStartOfWeek(hour_start) AS week_start,
    countIf(hourly_avg_price > 100) AS hours_above_100
FROM hourly_price
GROUP BY REGIONID, week_start
ORDER BY week_start, REGIONID