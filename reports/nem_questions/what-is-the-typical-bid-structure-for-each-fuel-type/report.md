        # What is the typical bid structure for each fuel type?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.bid_dayoffer`, reconciled to one row per settlement day, DUID, participant, and bid type, providing offered price bands and rebid explanations. `semantic.unit_dimension`, the current reconciled DUID dimension derived from MMSDM registration history, providing region, participant, station, fuel, registered capacity, storage flags, dispatch type, and emissions factor.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: FUEL_TYPE=Battery Storage, avg_priceband1=-958.74, avg_priceband5=99.09, avg_priceband10=16016.15. Returned 13 rows. The mean of `avg_priceband1` across the result set is -932.788.

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
    u.FUEL_TYPE,
    round(avg(PRICEBAND1), 2) AS avg_priceband1,
    round(avg(PRICEBAND5), 2) AS avg_priceband5,
    round(avg(PRICEBAND10), 2) AS avg_priceband10
FROM bid_day b
LEFT JOIN unit_dim u ON u.DUID = b.DUID
WHERE b.BIDTYPE = 'ENERGY'
GROUP BY u.FUEL_TYPE
ORDER BY u.FUEL_TYPE
        ```
