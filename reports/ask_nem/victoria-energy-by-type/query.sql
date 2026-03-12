SELECT
  toDate(g.INTERVAL_DATETIME) AS day,
  u.FUEL_TYPE AS fuel_type,
  SUM(g.MWH_READING) AS total_mwh
FROM semantic.actual_gen_duid AS g
JOIN semantic.unit_dimension AS u ON g.DUID = u.DUID
WHERE u.REGIONID = 'VIC1'
  AND g.INTERVAL_DATETIME >= now() - INTERVAL 7 DAY
GROUP BY day, fuel_type
ORDER BY day DESC, fuel_type
LIMIT 1000