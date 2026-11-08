SELECT * FROM (SELECT account_rank,origin_account,total_amount,
 total_amount/NULLIF(SUM(total_amount) OVER(),0) AS amount_share,
 SUM(total_amount) OVER(ORDER BY total_amount DESC,origin_account ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
 /NULLIF(SUM(total_amount) OVER(),0) AS cumulative_share
FROM (SELECT origin_account,SUM(amount) AS total_amount,
 ROW_NUMBER() OVER(ORDER BY SUM(amount) DESC,origin_account) AS account_rank
 FROM filtered GROUP BY origin_account)) WHERE account_rank<=20 ORDER BY account_rank
