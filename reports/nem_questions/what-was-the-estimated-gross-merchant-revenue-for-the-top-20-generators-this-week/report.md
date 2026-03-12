        # What was the estimated gross merchant revenue for the top 20 generators this week?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.dispatch_price`, reconciled to one row per settlement interval and region, providing 5-minute energy and FCAS regional price outcomes. `semantic.actual_gen_duid`, reconciled to one row per interval and DUID, representing metered energy readings used as actual generation. `semantic.unit_dimension`, the current reconciled DUID dimension derived from MMSDM registration history, providing region, participant, station, fuel, registered capacity, storage flags, dispatch type, and emissions factor.

        ## Note

        Gross merchant revenue is estimated as interval energy multiplied by regional spot price. This excludes contracts, settlement adjustments, losses, FCAS revenue, and bidding effects. Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: g.DUID=PIONEER, REGIONID=QLD1, FUEL_TYPE=Biomass and industrial materials, gross_revenue_proxy=6984.78. Returned 20 rows. The mean of `gross_revenue_proxy` across the result set is 3540.622.

        ## SQL

        ```sql
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
    g.DUID,
    any(u.REGIONID) AS REGIONID,
    any(u.FUEL_TYPE) AS FUEL_TYPE,
    round(sum(g.MWH_READING * p.RRP), 2) AS gross_revenue_proxy
FROM actual_gen g
INNER JOIN unit_dim u ON u.DUID = g.DUID
LEFT JOIN price_dispatch p ON p.SETTLEMENTDATE = g.INTERVAL_DATETIME AND p.REGIONID = u.REGIONID
WHERE g.INTERVAL_DATETIME >= date_trunc('week', now())
  AND coalesce(u.DISPATCHTYPE, '') = 'GENERATOR'
GROUP BY g.DUID
ORDER BY gross_revenue_proxy DESC
LIMIT 20
        ```
