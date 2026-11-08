SELECT simulation_step, transactions, total_amount,
 transactions-LAG(transactions) OVER(ORDER BY simulation_step) AS transaction_change,
 (transactions-LAG(transactions) OVER(ORDER BY simulation_step))*1.0/
 NULLIF(LAG(transactions) OVER(ORDER BY simulation_step),0) AS transaction_growth
FROM (SELECT simulation_step,COUNT(*) AS transactions,SUM(amount) AS total_amount
FROM filtered GROUP BY simulation_step) ORDER BY simulation_step
