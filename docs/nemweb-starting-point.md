# NEMweb Starting Point

## Proposed initial sources

Start with these two source families:

- `TradingIS`
  - `PRICE`
  - `INTERCONNECTORRES`
- `DispatchIS`
  - `LOCAL_PRICE`
  - `CASE_SOLUTION`

## Why this is the right first slice

This is the smallest useful set that can answer real portal questions:

- What was the regional price in NSW/VIC/QLD/SA/TAS at a given interval?
- How did prices differ between regions?
- What were interconnector flows and losses during a price event?
- Were local constraints causing local price adjustments for specific DUIDs?
- Was a dispatch run stressed or violating constraint categories?

It avoids the main trap in NEMweb first implementations: trying to ingest all MMS-style report families before there is a clear user-facing question set.

## Next sources to add after this

- `DISPATCH_UNIT_SCADA` or equivalent unit-level dispatch outputs
  - lets you answer generator and fuel-mix questions
- `PREDISPATCHIS`
  - lets you answer forecast versus actual questions
- rooftop PV / operational demand supporting datasets
  - lets you explain demand dynamics more accurately
