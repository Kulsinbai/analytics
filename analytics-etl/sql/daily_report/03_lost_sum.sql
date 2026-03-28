/* 03_lost_sum.sql
   Потенциально недополучено по отказам за вчера (по closed_at).
*/
WITH
  'Asia/Yekaterinburg' AS tz,
  (toDate(now(), tz) - 1) AS d
SELECT
  ifNull(sumIf(price, status_id = 143 AND price > 0), 0) AS lost_sum,
  countIf(status_id = 143 AND (price IS NULL OR price <= 0)) AS lost_budget_unknown_cnt
FROM default_db.leads_fact
WHERE client_id = {client_id:UInt32}
  AND pipeline_id = {pipeline_id:UInt32}
  AND closed_at IS NOT NULL
  AND toDate(closed_at, tz) = d
  AND status_id = 143