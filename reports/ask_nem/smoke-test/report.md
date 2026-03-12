        # Which participants own the most registered generation capacity?

        - Status: `answerable`
        - Confidence: `high`
        - Raw results: `results.csv`

        ## Data

        Top 20 participants by total registered generation capacity (MW), summed across all current DUIDs in the unit dimension.

        ## Note

        High confidence: totals are summed from semantic.unit_dimension REGISTEREDCAPACITY_MW (>0) by EFFECTIVE_PARTICIPANTID across all current DUIDs. Figures represent registered nameplate/discharge capability, not real-time availability or energy-limited output. This is a current-state snapshot (de-registered units excluded) and may include storage discharge and special/programmatic registrations (e.g., BASSLINK, VBBHMCA3, NEMRESTR), so interpret rankings as per MMS participant ownership rather than technology groupings.

        ## Explanation

        Based on current MMS registrations, the participant with the most total registered generation capacity is SNOWY (33,554 MW), followed by NEMRESTR (30,000 MW). The next largest owners are MACQGEN (5,573 MW), CSENERGY (4,115 MW), STANWELL (3,783 MW), HYDROTAS (3,625 MW), and CLEANCO (3,175 MW). The top 20 also include OERARING, BORAL, TIPSCO, TRUDW, LOYYANGA, DELTA, HAZELPWR, YALLOURN, BBPEM, SECV, WDOWBESS, BASSLINK, and VBBHMCA3.

        ## SQL

        ```sql
        SELECT
  coalesce(EFFECTIVE_PARTICIPANTID, PARTICIPANTID) AS PARTICIPANT,
  sum(REGISTEREDCAPACITY_MW) AS TOTAL_REGISTERED_CAPACITY_MW
FROM semantic.unit_dimension
WHERE REGISTEREDCAPACITY_MW > 0
GROUP BY coalesce(EFFECTIVE_PARTICIPANTID, PARTICIPANTID)
ORDER BY TOTAL_REGISTERED_CAPACITY_MW DESC
LIMIT 20;
        ```
