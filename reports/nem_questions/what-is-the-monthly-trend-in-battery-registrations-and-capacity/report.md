        # What is the monthly trend in battery registrations and capacity?

        - Status: `ready`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.participant_registration_dudetail`, a historical MMSDM registration view with effective-dated DUID registration details.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: month_start=2024-07-01, battery_registrations=8, registered_capacity_mw=619.0. Returned 20 rows. The mean of `battery_registrations` across the result set is 3.050.

        ## SQL

        ```sql

WITH battery_events AS (
    SELECT
        toStartOfMonth(EFFECTIVEDATE) AS month_start,
        DUID,
        avg(REGISTEREDCAPACITY) AS registered_capacity_mw
    FROM semantic.participant_registration_dudetail
    WHERE MAXSTORAGECAPACITY > 0 OR DISPATCHTYPE = 'BIDIRECTIONAL'
    GROUP BY month_start, DUID
)
SELECT
    month_start,
    countDistinct(DUID) AS battery_registrations,
    round(sum(registered_capacity_mw), 2) AS registered_capacity_mw
FROM battery_events
GROUP BY month_start
ORDER BY month_start
        ```
