# Which generators responded to price spikes and how quickly?

- Status: `blocked`
- Chart: `chart.png`
- Raw results: `results.csv`

## Data

No production semantic fact or dimension could be queried for this report. The SQL bundle is a diagnostic placeholder because the required dataset or modeled surface is not yet available in ClickHouse.

## Note

Response speed to price spikes is not yet implemented in the report harness. Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

## Explanation

The question is currently blocked because the required data product or modeled analytic surface is not available.

## SQL

```sql
SELECT 'blocked' AS status, 'This needs event-window response logic with pre- and post-spike ramp comparison.' AS reason
```
