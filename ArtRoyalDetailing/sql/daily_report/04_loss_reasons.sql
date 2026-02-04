/* 04_loss_reasons.sql
   Причины отказов по проигранным (status_id = 143) среди созданных вчера.
   Включает строку "Причины отказов не заполнены".
*/
WITH
  'Asia/Yekaterinburg' AS tz,
  toDate(yesterday(), tz) AS d
SELECT
  reason,
  cnt,
  sum_price
FROM
(
  /* Заполненные причины */
  SELECT
    lr.loss_reason_name AS reason,
    count() AS cnt,
    ifNull(sumIf(l.price, l.price > 0), 0) AS sum_price
  FROM default_db.leads_fact l
  LEFT JOIN default_db.loss_reasons_dim_v2 lr
    ON l.client_id = lr.client_id
   AND l.loss_reason_id = lr.loss_reason_id
  WHERE l.client_id = {client_id:UInt32}
    AND toDate(l.created_at, tz) = d
    AND l.status_id = 143
    AND l.loss_reason_id IS NOT NULL
    AND l.loss_reason_id != 0
  GROUP BY reason

  UNION ALL

  /* Пустые причины */
  SELECT
    'Причины отказов не заполнены' AS reason,
    count() AS cnt,
    ifNull(sumIf(price, price > 0), 0) AS sum_price
  FROM default_db.leads_fact
  WHERE client_id = {client_id:UInt32}
    AND toDate(created_at, tz) = d
    AND status_id = 143
    AND (loss_reason_id IS NULL OR loss_reason_id = 0)
)
WHERE cnt > 0
ORDER BY cnt DESC, sum_price DESC, reason ASC;
