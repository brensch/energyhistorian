# Which participants own the most registered generation capacity?

- Status: `answerable`
- Confidence: `high`
- Raw results: `results.csv`

## Data

Summed current registered capacity (MW) from the reconciled unit_dimension, aggregated by PARTICIPANTID. Includes a count of DUIDs contributing to each participant's total.

## Note

This uses the current-state REGISTEREDCAPACITY_MW in semantic.unit_dimension (confidence: high for this aggregation). It reflects effective/current registered ownership, not historical ownership changes or actual operational availability. Large or unexpected participant totals (e.g. NEMRESTR) may represent special/system-level records or aggregated entries—verify participant codes and meanings before acting on them. For historised ownership or validated settlement-grade data, join to participant_registration_stationowner or participant_registration_dudetail and cross-check against operational/settlement datasets.

## Explanation

The result aggregates current reconciled REGISTEREDCAPACITY_MW from semantic.unit_dimension by PARTICIPANTID and counts contributing DUIDs. The largest registered-capacity owners in the preview are: SNOWY (33,388.0 MW; 74 DUIDs), NEMRESTR (30,000.0 MW; 31 DUIDs), MACQGEN (5,573.0 MW; 17 DUIDs), CSENERGY (4,942.0 MW; 41 DUIDs) and STANWELL (4,083.0 MW; 19 DUIDs). Values are summed current registered capacity per participant as stored in unit_dimension.

## SQL

```sql
SELECT
  PARTICIPANTID,
  sum(REGISTEREDCAPACITY_MW) AS total_registered_capacity_mw,
  count(DUID) AS duid_count
FROM semantic.unit_dimension
WHERE PARTICIPANTID IS NOT NULL
GROUP BY PARTICIPANTID
ORDER BY total_registered_capacity_mw DESC
LIMIT 20;
```
