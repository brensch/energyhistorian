        # Which batteries had the highest estimated arbitrage revenue this week?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.dispatch_price`, reconciled to one row per settlement interval and region, providing 5-minute energy and FCAS regional price outcomes. `semantic.daily_unit_dispatch`, reconciled to one row per settlement interval and DUID, providing dispatch targets, initial MW, availability, ramp rates, and FCAS enablement. `semantic.unit_dimension`, the current reconciled DUID dimension derived from MMSDM registration history, providing region, participant, station, fuel, registered capacity, storage flags, dispatch type, and emissions factor.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: d.DUID=BLYTHB1, REGIONID=SA1, arbitrage_revenue_proxy=4374.43. Returned 20 rows. The mean of `arbitrage_revenue_proxy` across the result set is 527.142.

        ## SQL

        ```sql
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
    d.DUID,
    any(u.REGIONID) AS REGIONID,
    round(sum(
        CASE
            WHEN d.TOTALCLEARED >= 0 THEN (d.TOTALCLEARED / 12.0) * p.RRP
            ELSE -(abs(d.TOTALCLEARED) / 12.0) * p.RRP
        END
    ), 2) AS arbitrage_revenue_proxy
FROM unit_dispatch d
INNER JOIN unit_dim u ON u.DUID = d.DUID
LEFT JOIN price_dispatch p ON p.SETTLEMENTDATE = d.SETTLEMENTDATE AND p.REGIONID = u.REGIONID
WHERE d.SETTLEMENTDATE >= date_trunc('week', now())
  AND u.IS_STORAGE = 1
GROUP BY d.DUID
ORDER BY arbitrage_revenue_proxy DESC
LIMIT 20
        ```
