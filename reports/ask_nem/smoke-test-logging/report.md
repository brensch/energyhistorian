# Which participants own the most registered generation capacity?

- Status: `answerable`
- Confidence: `high`
- Raw results: `results.csv`

## Data

Current reconciled registered capacity (MW) per participant from semantic.unit_dimension (one row per DUID in current effective state).

## Note

Method: sum of REGISTEREDCAPACITY_MW grouped by PARTICIPANTID from semantic.unit_dimension (current effective state). Caveats: this is a snapshot of registered capacity, not necessarily available or operational capacity, and does not provide historical ownership changes. For time‑series or settlement‑grade ownership or registration details, use the MMSDM registration views (e.g. participant_registration_stationowner, participant_registration_genunits). The aggregation is high confidence for the current-state data exposed here, but verify with source registration views for formal/settlement purposes.

## Explanation

This result is a current-state aggregation of REGISTEREDCAPACITY_MW by PARTICIPANTID from semantic.unit_dimension (one row per DUID in the effective state). The largest registered-capacity owners in the snapshot are: SNOWY (33,388 MW), NEMRESTR (30,000 MW), MACQGEN (5,573 MW), CSENERGY (4,942 MW) and STANWELL (4,083 MW). The result set contains 50 participants (preview shows the top 20). Values are summed registered capacity in MW.

## SQL

```sql
SELECT
  PARTICIPANTID,
  SUM(REGISTEREDCAPACITY_MW) AS total_registered_mw
FROM semantic.unit_dimension
GROUP BY PARTICIPANTID
ORDER BY total_registered_mw DESC
LIMIT 50
```
