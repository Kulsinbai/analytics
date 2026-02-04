/* 03_lost_sum.sql
   Потенциально недополучено по проигранным (status_id = 143)
   среди созданных вчера + сколько из них с неизвестным бюджетом.
*/
WITH
  'Asia/Yekaterinburg' AS tz,
  toDate(yesterday(), tz) AS d
SELECT
  ifNull(sumIf(price, status_id = 143 AND price > 0), 0) AS lost_sum,
  countIf(status_id = 143 AND (price IS NULL OR price <= 0)) AS lost_budget_unknown_cnt
FROM default_db.leads_fact
WHERE client_id = {client_id:UInt32}
  AND toDate(created_at, tz) = d;
