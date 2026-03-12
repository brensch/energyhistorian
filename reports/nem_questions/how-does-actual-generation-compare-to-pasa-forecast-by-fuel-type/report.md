# How does actual generation compare to PASA forecast by fuel type?

- Status: `blocked`
- Chart: `chart.png`
- Raw results: `results.csv`

## Data

No production semantic fact or dimension could be queried for this report. The SQL bundle is a diagnostic placeholder because the required dataset or modeled surface is not yet available in ClickHouse.

## Note

The current semantic layer does not yet expose a clean PASA-versus-actual comparison surface. Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

## Explanation

The question is currently blocked because the required data product or modeled analytic surface is not available.

## SQL

```sql
SELECT 'blocked' AS status, 'PASA-to-actual comparison needs a reconciled fuel-typed PASA fact mart and clean alignment logic between PASA availability and interval generation.' AS reason
```
