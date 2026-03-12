        # Which participants own the most registered generation capacity?

        - Status: `answerable`
        - Confidence: `high`
        - Raw results: `results.csv`

        ## Data

        One row per participant with the sum of current-state REGISTEREDCAPACITY_MW from the reconciled unit_dimension (total registered generation capacity in MW).

        ## Note

        This uses the reconciled current PARTICIPANTID in semantic.unit_dimension — suitable for understanding present ownership. If you need owner-at-a-specific-date (historical ownership) query the historised participant/registration tables instead. Data is reported in MW and the plan indicates high confidence in this current reconciled view, but it is not a settlement-grade certification; verify against official AEMO records for settlement or contractual decisions.

        ## Explanation

        The query sums current-state REGISTEREDCAPACITY_MW (MW) per PARTICIPANTID from semantic.unit_dimension. The top owners in the returned preview are: SNOWY (33,388.0 MW), NEMRESTR (30,000.0 MW), MACQGEN (5,573.0 MW), CSENERGY (4,942.0 MW) and STANWELL (4,083.0 MW). The result set contains 20 participants (preview shown).

        ## SQL

        ```sql
        SELECT
  PARTICIPANTID,
  SUM(REGISTEREDCAPACITY_MW) AS total_registered_capacity_mw
FROM semantic.unit_dimension
WHERE PARTICIPANTID IS NOT NULL
  AND REGISTEREDCAPACITY_MW IS NOT NULL
GROUP BY PARTICIPANTID
ORDER BY total_registered_capacity_mw DESC
LIMIT 20;
        ```
