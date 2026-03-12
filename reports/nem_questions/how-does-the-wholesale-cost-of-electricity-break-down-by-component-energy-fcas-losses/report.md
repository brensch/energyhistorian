# How does the wholesale cost of electricity break down by component (energy, FCAS, losses)?

- Status: `blocked`
- Chart: `chart.png`
- Raw results: `results.csv`

## Data

No production semantic fact or dimension could be queried for this report. The SQL bundle is a diagnostic placeholder because the required dataset or modeled surface is not yet available in ClickHouse.

## Note

The semantic layer does not yet expose settlement-grade cost components. Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

## Explanation

The question is currently blocked because the required data product or modeled analytic surface is not available.

## SQL

```sql
SELECT 'blocked' AS status, 'A component breakdown needs an explicit settlement mart for energy, FCAS, and losses.' AS reason
```
