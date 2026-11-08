SELECT COUNT(*) AS transactions, COALESCE(SUM(amount),0) AS total_amount,
 AVG(amount) AS average_amount, percentile_approx(amount,0.5,10000) AS median_amount,
 MIN(amount) AS minimum_amount, MAX(amount) AS maximum_amount,
 COALESCE(SUM(is_fraud),0) AS fraud_transactions,
 AVG(is_fraud) AS fraud_rate,
 COALESCE(SUM(CASE WHEN is_fraud=1 THEN amount ELSE 0 END),0) AS fraud_amount,
 SUM(CASE WHEN is_fraud=1 THEN amount ELSE 0 END)/NULLIF(SUM(amount),0) AS fraud_amount_share,
 COUNT(DISTINCT origin_account) AS origin_accounts,
 COUNT(DISTINCT destination_account) AS destination_accounts,
 MIN(simulation_step) AS first_hour, MAX(simulation_step) AS last_hour
FROM filtered
