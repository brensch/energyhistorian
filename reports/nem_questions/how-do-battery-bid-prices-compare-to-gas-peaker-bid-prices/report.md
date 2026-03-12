        # How do battery bid prices compare to gas peaker bid prices?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.bid_dayoffer`, reconciled to one row per settlement day, DUID, participant, and bid type, providing offered price bands and rebid explanations. `semantic.unit_dimension`, the current reconciled DUID dimension derived from MMSDM registration history, providing region, participant, station, fuel, registered capacity, storage flags, dispatch type, and emissions factor.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: technology=Battery, avg_priceband1=-924.53, avg_priceband5=95.69, avg_priceband10=15695.83. Returned 2 rows. The mean of `avg_priceband1` across the result set is -950.195.

        ## SQL

        ```sql
        WITH

bid_day AS (
    SELECT
        SETTLEMENTDATE,
        DUID,
        PARTICIPANTID,
        BIDTYPE,
        avg(VERSIONNO) AS VERSIONNO,
        avg(PRICEBAND1) AS PRICEBAND1,
        avg(PRICEBAND2) AS PRICEBAND2,
        avg(PRICEBAND3) AS PRICEBAND3,
        avg(PRICEBAND4) AS PRICEBAND4,
        avg(PRICEBAND5) AS PRICEBAND5,
        avg(PRICEBAND6) AS PRICEBAND6,
        avg(PRICEBAND7) AS PRICEBAND7,
        avg(PRICEBAND8) AS PRICEBAND8,
        avg(PRICEBAND9) AS PRICEBAND9,
        avg(PRICEBAND10) AS PRICEBAND10,
        anyLast(REBIDEXPLANATION) AS REBIDEXPLANATION
    FROM semantic.bid_dayoffer
    GROUP BY SETTLEMENTDATE, DUID, PARTICIPANTID, BIDTYPE
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
    CASE
        WHEN u.IS_STORAGE = 1 THEN 'Battery'
        WHEN lower(u.FUEL_TYPE) = 'natural gas (pipeline)' THEN 'Gas peaker'
        ELSE 'Other'
    END AS technology,
    round(avg(PRICEBAND1), 2) AS avg_priceband1,
    round(avg(PRICEBAND5), 2) AS avg_priceband5,
    round(avg(PRICEBAND10), 2) AS avg_priceband10
FROM bid_day b
LEFT JOIN unit_dim u ON u.DUID = b.DUID
WHERE b.BIDTYPE = 'ENERGY'
  AND (u.IS_STORAGE = 1 OR lower(u.FUEL_TYPE) = 'natural gas (pipeline)')
GROUP BY technology
ORDER BY technology
        ```
