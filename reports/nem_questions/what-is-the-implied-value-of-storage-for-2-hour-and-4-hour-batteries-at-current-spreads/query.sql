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

daily_spreads AS (
    SELECT
        toDate(SETTLEMENTDATE) AS trading_day,
        REGIONID,
        max(RRP) - min(RRP) AS intraday_spread
    FROM price_dispatch
    GROUP BY trading_day, REGIONID
)

SELECT
    trading_day,
    REGIONID,
    round(intraday_spread * 2, 2) AS implied_value_2h_mwday,
    round(intraday_spread * 4, 2) AS implied_value_4h_mwday
FROM daily_spreads
ORDER BY trading_day, REGIONID