        # How did generators change their bids day-over-day?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.bid_dayoffer`, reconciled to one row per settlement day, DUID, participant, and bid type, providing offered price bands and rebid explanations.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: SETTLEMENTDATE=2026-03-02 00:00:00, DUID=ADPBA1, avg_bid_band_price=728.5. Returned 4,958 rows. The mean of `avg_bid_band_price` across the result set is 2519.259.

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
    SETTLEMENTDATE,
    DUID,
    avg(PRICEBAND1 + PRICEBAND2 + PRICEBAND3 + PRICEBAND4 + PRICEBAND5 + PRICEBAND6 + PRICEBAND7 + PRICEBAND8 + PRICEBAND9 + PRICEBAND10) / 10.0 AS avg_bid_band_price
FROM bid_day
WHERE BIDTYPE = 'ENERGY'
GROUP BY SETTLEMENTDATE, DUID
ORDER BY SETTLEMENTDATE, DUID
        ```
