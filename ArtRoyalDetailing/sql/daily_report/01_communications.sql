/* 01_communications.sql
   Коммуникации за вчера (созданы вчера), группируем по source.
*/
WITH
  'Asia/Yekaterinburg' AS tz,
  toDate(yesterday(), tz) AS d
SELECT
  coalesce(nullIf(source, ''), nullIf(channel, ''), 'прочее') AS source,
  count() AS cnt
FROM default_db.leads_fact
WHERE client_id = {client_id:UInt32}
  AND toDate(created_at, tz) = d
  AND (source IS NOT NULL OR channel IS NOT NULL)
GROUP BY source
HAVING cnt > 0
ORDER BY cnt DESC, source ASC;
