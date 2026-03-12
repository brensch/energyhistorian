        # How has the number of registered DUIDs changed over the last 5 years?

        - Status: `ready`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.participant_registration_dudetail`, a historical MMSDM registration view with effective-dated DUID registration details. `semantic.participant_registration_dudetailsummary`, a historical MMSDM summary view for DUID registration periods and start dates.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: year_start=2021-01-01, registered_duids=585. Returned 6 rows. The mean of `registered_duids` across the result set is 567.167.

        ## SQL

        ```sql

SELECT
    toStartOfYear(START_DATE) AS year_start,
    countDistinct(DUID) AS registered_duids
FROM semantic.participant_registration_dudetailsummary
WHERE START_DATE >= now() - INTERVAL 5 YEAR
GROUP BY year_start
ORDER BY year_start
        ```
