SELECT simulation_step, simulation_day_index, hour_index_in_simulated_day,
       COUNT(*) AS transactions, SUM(amount) AS total_amount,
       SUM(is_fraud) AS fraud_transactions
FROM {{STAR_VIEW}}
GROUP BY simulation_step, simulation_day_index, hour_index_in_simulated_day
ORDER BY simulation_step;
