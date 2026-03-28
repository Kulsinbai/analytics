/* 02_sales.sql
   Продажи и отказы за вчера по дате закрытия (closed_at).
   142 = успешно
   143 = отказ
*/
WITH
  'Asia/Yekaterinburg' AS tz,
  (toDate(now(), tz) - 1) AS d
SELECT
  countIf(status_id = 142) AS won_cnt,
  ifNull(sumIf(price, status_id = 142 AND price > 0), 0) AS won_sum,
  countIf(status_id = 143) AS lost_cnt
FROM default_db.leads_fact
WHERE client_id = {client_id:UInt32}
  AND pipeline_id = {pipeline_id:UInt32}
  AND closed_at IS NOT NULL
  AND toDate(closed_at, tz) = d
  AND status_id IN (142, 143)