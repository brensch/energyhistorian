SELECT
  toDate(d.SETTLEMENTDATE) AS day,
  u.FUEL_TYPE AS type,
  sum(greatest(d.TOTALCLEARED, 0) / 12.0) AS energy_mwh
FROM semantic.daily_unit_dispatch AS d
JOIN semantic.unit_dimension AS u ON d.DUID = u.DUID
WHERE u.REGIONID = 'VIC1'
  AND toDate(d.SETTLEMENTDATE) >= today() - 6
GROUP BY day, type
ORDER BY day DESC, type ASC