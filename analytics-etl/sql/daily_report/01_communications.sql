/* 01_communications.sql
   Обращения за вчера по created_at, группировка по source.
   С учётом pipeline_id.
*/
WITH
  'Asia/Yekaterinburg' AS tz,
  (toDate(now(), tz) - 1) AS d
SELECT
  source,
  count() AS cnt
FROM default_db.leads_fact
WHERE client_id = {client_id:UInt32}
  AND pipeline_id = {pipeline_id:UInt32}
  AND toDate(created_at, tz) = d
  AND source IS NOT NULL
  AND source != ''
GROUP BY source
ORDER BY cnt DESC, source ASC