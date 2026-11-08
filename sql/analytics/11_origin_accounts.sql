SELECT origin_account,COUNT(*) AS transactions,SUM(amount) AS total_amount,
 COUNT(DISTINCT destination_account) AS distinct_counterparties,SUM(is_fraud) AS fraud_transactions,
 SUM(CASE WHEN is_fraud=1 THEN amount ELSE 0 END) AS fraud_amount
FROM filtered GROUP BY origin_account ORDER BY total_amount DESC,origin_account LIMIT 20
