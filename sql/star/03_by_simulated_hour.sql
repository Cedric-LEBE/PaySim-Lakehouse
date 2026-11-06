SELECT d.simulation_step, d.simulation_day_index, d.hour_index_in_simulated_day,
       COUNT(*) AS transactions, SUM(f.amount) AS total_amount, SUM(f.is_fraud) AS fraud_transactions
FROM fact_transaction f
JOIN dim_simulation_time d ON f.simulation_time_key = d.simulation_time_key
GROUP BY d.simulation_step, d.simulation_day_index, d.hour_index_in_simulated_day
ORDER BY d.simulation_step;
