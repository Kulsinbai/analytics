/* 02_sales.sql
   Продажи и проигрыши среди сделок, созданных вчера.
   - won_cnt: сколько сейчас в статусе 142
   - won_sum: сумма price по успешным (только price > 0)
   - lost_cnt: сколько сейчас в статусе 143
*/
WITH
  'Asia/Yekaterinburg' AS tz,
  toDate(yesterday(), tz) AS d
SELECT
  countIf(status_id = 142) AS won_cnt,
  ifNull(sumIf(price, status_id = 142 AND price > 0), 0) AS won_sum,
  countIf(status_id = 143) AS lost_cnt
FROM default_db.leads_fact
WHERE client_id = {client_id:UInt32}
  AND toDate(created_at, tz) = d;
