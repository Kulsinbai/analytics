/* 04_loss_reasons.sql
   Причины отказов по отказам за вчера (по closed_at).
*/
WITH
  'Asia/Yekaterinburg' AS tz,
  (toDate(now(), tz) - 1) AS d
SELECT
  reason,
  cnt,
  sum_price
FROM
(
  /* Заполненные причины */
  SELECT
    coalesce(lr.loss_reason_name, 'Причина не найдена в справочнике') AS reason,
    count() AS cnt,
    ifNull(sumIf(l.price, l.price > 0), 0) AS sum_price
  FROM default_db.leads_fact l
  LEFT JOIN default_db.loss_reasons_dim_v2 lr
    ON l.client_id = lr.client_id
   AND l.loss_reason_id = lr.loss_reason_id
  WHERE l.client_id = {client_id:UInt32}
    AND l.pipeline_id = {pipeline_id:UInt32}
    AND l.closed_at IS NOT NULL
    AND toDate(l.closed_at, tz) = d
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
    AND pipeline_id = {pipeline_id:UInt32}
    AND closed_at IS NOT NULL
    AND toDate(closed_at, tz) = d
    AND status_id = 143
    AND (loss_reason_id IS NULL OR loss_reason_id = 0)
)
WHERE cnt > 0
ORDER BY cnt DESC, sum_price DESC, reason ASC