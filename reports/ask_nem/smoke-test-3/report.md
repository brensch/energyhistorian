        # Which participants own the most registered generation capacity?

        - Status: `answerable`
        - Confidence: `high`
        - Raw results: `results.csv`

        ## Data

        Aggregates the registered generation capacity in MW by participant from the current unit dimension data reflecting MMSDM registration.

        ## Note

        The results are derived from the unit_dimension object, which contains the most up-to-date and reconciled registered generation capacity per DUID and participant. While this is a high-confidence proxy for ownership, the data should not be considered settlement-grade, as it reflects registered capacity rather than real-time dispatched or operational capacity.

        ## Explanation

        The analysis of the semantic.unit_dimension data source shows that the participant with the largest registered generation capacity is SNOWY, owning 33,388 MW. The next largest owner is NEMRESTR with 30,000 MW. Other notable participants include MACQGEN with 5,573 MW and CSENERGY with 4,942 MW. These figures reflect current MMSDM reconciled capacities and provide a reliable proxy for ownership of registered generation assets in the NEM.

        ## SQL

        ```sql
        SELECT PARTICIPANTID, SUM(REGISTEREDCAPACITY_MW) AS TOTAL_REGISTERED_CAPACITY_MW
FROM semantic.unit_dimension
GROUP BY PARTICIPANTID
ORDER BY TOTAL_REGISTERED_CAPACITY_MW DESC
LIMIT 10
        ```
