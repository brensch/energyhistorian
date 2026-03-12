SELECT
  toDate(a.INTERVAL_DATETIME) AS day,
  u.FUEL_TYPE AS fuel_type,
  sum(a.MWH_READING) AS energy_mwh
FROM semantic.actual_gen_duid AS a
JOIN semantic.unit_dimension AS u
  ON a.DUID = u.DUID
WHERE u.REGIONID = 'VIC'
  AND toDate(a.INTERVAL_DATETIME) >= today() - 6
GROUP BY day, fuel_type
ORDER BY day DESC, fuel_type