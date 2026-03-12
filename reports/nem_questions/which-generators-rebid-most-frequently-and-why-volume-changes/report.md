        # Which generators rebid most frequently and why (volume changes)?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.bid_dayoffer`, reconciled to one row per settlement day, DUID, participant, and bid type, providing offered price bands and rebid explanations.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: DUID=RT_NSW2, rebid_count=10, latest_rebid_explanation=5MS - INITIAL RESERVER TRADER DEFAULT BID. Returned 25 rows. The mean of `rebid_count` across the result set is 10.000.

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

SELECT
    DUID,
    count() AS rebid_count,
    anyLast(REBIDEXPLANATION) AS latest_rebid_explanation
FROM bid_day
WHERE BIDTYPE = 'ENERGY'
  AND REBIDEXPLANATION IS NOT NULL
  AND REBIDEXPLANATION != ''
GROUP BY DUID
ORDER BY rebid_count DESC
LIMIT 25
        ```
