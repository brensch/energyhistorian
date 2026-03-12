SELECT
  toDate(dud.SETTLEMENTDATE) AS day,
  ud.FUEL_TYPE AS fuel_type,
  sum(greatest(dud.TOTALCLEARED, 0) / 12.0) AS energy_mwh
FROM semantic.daily_unit_dispatch AS dud
JOIN semantic.unit_dimension AS ud
  ON dud.DUID = ud.DUID
WHERE ud.REGIONID = 'VIC1'
  AND dud.SETTLEMENTDATE >= now() - INTERVAL 7 DAY
GROUP BY day, ud.FUEL_TYPE
ORDER BY day DESC, ud.FUEL_TYPE