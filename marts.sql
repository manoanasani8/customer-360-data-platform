CREATE OR REPLACE VIEW current_customers AS
SELECT customer_id, version, segment, region, is_deleted
FROM customer_history
WHERE valid_to IS NULL;

CREATE OR REPLACE VIEW transaction_detail AS
SELECT t.transaction_id, t.customer_id, t.occurred_at, t.amount_cents,
       h.version AS customer_version, h.segment, h.region
FROM transactions t
JOIN customer_history h
  ON t.customer_id = h.customer_id
 AND t.occurred_at >= h.valid_from
 AND (h.valid_to IS NULL OR t.occurred_at < h.valid_to)
 AND NOT h.is_deleted;

CREATE OR REPLACE VIEW segment_activity AS
SELECT CAST(occurred_at AS DATE) AS activity_date, segment, region,
       COUNT(*) AS transaction_count, SUM(amount_cents)::BIGINT AS amount_cents
FROM transaction_detail
GROUP BY activity_date, segment, region;
