        # Which participants own the most registered generation capacity?

        - Status: `answerable`
        - Confidence: `high`
        - Raw results: `results.csv`

        ## Data

        Aggregated the sum of registered generation capacity in MW for each participant from the current reconciled DUID dimension.

        ## Note

        The capacity figures are based on the current effective registered capacities from semantic.unit_dimension, considered a high-confidence authoritative source. However, these numbers represent registered capacities and may not reflect actual operational availability or recent changes outside the effective dataset. This summary should not be considered settlement-grade but provides a reliable overview of ownership distribution.

        ## Explanation

        The participants owning the most registered generation capacity are ranked by their total registered capacity in megawatts (MW). SNOWY leads with 33,388 MW, followed by NEMRESTR with 30,000 MW. Other significant owners include MACQGEN with 5,573 MW and CSENERGY with 4,942 MW. This data was aggregated from the current reconciled unit dimension, which records participant ownership and registered capacities per generation unit.

        ## SQL

        ```sql
        SELECT PARTICIPANTID, SUM(REGISTEREDCAPACITY_MW) AS TOTAL_REGISTERED_CAPACITY_MW
FROM semantic.unit_dimension
GROUP BY PARTICIPANTID
ORDER BY TOTAL_REGISTERED_CAPACITY_MW DESC
LIMIT 10
        ```
