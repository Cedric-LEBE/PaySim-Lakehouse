"""Execute all dashboard analyses with bound filters and reconcile independent totals."""
from decimal import Decimal
import json
from pathlib import Path
from pyspark import StorageLevel
from pyspark.sql import functions as F
from banking.bronze import session
from banking.star import register_views

spark=session()
spark.sparkContext.setLogLevel('WARN')
wide=None
try:
    register_views(spark,'data/gold/star')
    wide=spark.sql('''SELECT f.*,ty.transaction_type,tm.simulation_step,tm.simulation_day_index,
        tm.hour_index_in_simulated_day,o.account_id AS origin_account,d.account_id AS destination_account
        FROM fact_transaction f JOIN dim_transaction_type ty ON f.transaction_type_key=ty.transaction_type_key
        JOIN dim_simulation_time tm ON f.simulation_time_key=tm.simulation_time_key
        JOIN dim_account o ON f.origin_account_key=o.account_key
        JOIN dim_account d ON f.destination_account_key=d.account_key''').persist(StorageLevel.DISK_ONLY)
    wide.createOrReplaceTempView('transactions_dashboard')
    prefix=Path('sql/analytics/filter.sql').read_text()
    files=sorted(Path('sql/analytics').glob('[0-9]*.sql'))
    defaults={'types':[],'fraud_labels':[],'hour_min':'1','hour_max':'744','amount_min':'0','amount_max':'999999999999999999.99'}
    def run(params):
        return {p.stem:spark.sql(prefix+p.read_text(),args=params).collect() for p in files}
    results=run(defaults)
    summary=results['01_summary'][0]
    assert summary.transactions==wide.count()==250000
    assert summary.total_amount==Decimal('44796585140.23') and summary.fraud_transactions==163
    for name in ['02_by_type','03_hourly','06_amount_distribution','07_fraud_comparison']:
        assert sum(r.transactions for r in results[name])==summary.transactions,name
        assert sum((r.total_amount for r in results[name]),Decimal(0))==summary.total_amount,name
    assert sum(r.transactions for r in results['09_flag_matrix'])==summary.transactions
    metrics=results['10_flag_metrics'][0]
    assert metrics.tp+metrics.fn==summary.fraud_transactions
    assert len(results['11_origin_accounts'])<=20 and len(results['16_transactions'])<=500
    shares=[r.cumulative_share for r in results['13_concentration']]
    assert shares==sorted(shares) and all(0<=v<=1 for v in shares)
    filtered={**defaults,'types':['TRANSFER'],'fraud_labels':['Non-fraudulent'],'hour_min':'2','hour_max':'4','amount_min':'0.11','amount_max':'100000.33'}
    narrowed=run(filtered)
    expected=wide.filter((F.col('transaction_type')=='TRANSFER') & (F.col('is_fraud')==0) & F.col('simulation_step').between(2,4) & F.col('amount').between(Decimal('0.11'),Decimal('100000.33')))
    expected_total=expected.agg(F.count('*').alias('rows'),F.sum('amount').alias('amount')).first()
    assert narrowed['01_summary'][0].transactions==expected_total.rows
    assert narrowed['01_summary'][0].total_amount==(expected_total.amount or 0)
    assert narrowed['01_summary'][0].fraud_transactions==0
    empty=run({**defaults,'hour_min':'745','hour_max':'744'})
    assert empty['01_summary'][0].transactions==0 and empty['01_summary'][0].fraud_rate is None
    assert empty['10_flag_metrics'][0].precision_flag is None
    print('PASS: 16 analyses sur 250 000 faits, réconciliations, filtres combinés et cas vide validés.',flush=True)
    print(json.dumps({'transactions':summary.transactions,'frauds':summary.fraud_transactions,'first_hour':summary.first_hour,'last_hour':summary.last_hour,'filtered_rows':expected_total.rows},default=str),flush=True)
finally:
    if wide is not None: wide.unpersist()
    spark.stop()
