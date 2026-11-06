"""Reconcile a bounded PaySim replay with Silver, Gold and the published star."""
import argparse
from pathlib import Path
from decimal import Decimal
from collections import defaultdict
from banking.bronze import session
from banking.producer import records
from banking.silver import DATASET
from banking.star import register_views
from pyspark.sql import functions as F

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--max-messages', type=int, default=10000)
args = parser.parse_args()
if args.max_messages <= 0:
    parser.error('max-messages doit être positif')

spark = session()
spark.sparkContext.setLogLevel('WARN')
try:
    expected = list(records(Path('data/raw/PS_20174392719_1491204439457_log.csv'), DATASET, args.max_messages))
    assert len(expected) == args.max_messages, 'CSV plus court que le périmètre demandé'
    silver = spark.read.format('delta').load('data/silver/transactions')
    actual = silver.select('event_id', 'source_row', 'step', 'type', 'amount', 'nameOrig', 'oldbalanceOrg', 'newbalanceOrig', 'nameDest', 'oldbalanceDest', 'newbalanceDest', 'isFraud', 'isFlaggedFraud').collect()
    indexed = {r['event_id']: r for r in expected}
    assert len(actual) == len(indexed) == args.max_messages
    assert {r.event_id for r in actual} == set(indexed)
    money = {'amount', 'oldbalanceOrg', 'newbalanceOrig', 'oldbalanceDest', 'newbalanceDest'}
    for row in actual:
        ref = indexed[row.event_id]
        assert row.source_row == ref['source_row']
        for k, value in ref['transaction'].items():
            assert row[k] == (Decimal(value) if k in money else value), (row.event_id, k)
    for table, key in [('summary', None), ('by_type', 'type'), ('by_step', 'step')]:
        grouped = defaultdict(list)
        for e in expected:
            t = e['transaction']
            grouped[t[key] if key else None].append(t)
        output = spark.read.format('delta').load('data/gold/' + table).collect()
        assert len(output) == len(grouped)
        for r in output:
            refs = grouped[r[key] if key else None]
            assert r.total_transactions == len(refs)
            assert r.total_amount == sum((Decimal(t['amount']) for t in refs), Decimal(0))
            assert r.fraud_transactions == sum(t['isFraud'] for t in refs)
            assert r.fraud_amount == sum((Decimal(t['amount']) for t in refs if t['isFraud']), Decimal(0))
    manifest = register_views(spark, 'data/gold/star')
    fact = spark.table('fact_transaction')
    assert fact.count() == fact.select('transaction_key').distinct().count() == args.max_messages
    assert fact.select('transaction_key').subtract(silver.select('event_id')).count() == 0
    totals = fact.agg(F.sum('amount').alias('amount'), F.sum('is_fraud').alias('frauds')).first()
    assert totals.amount == sum((Decimal(e['transaction']['amount']) for e in expected), Decimal(0))
    assert totals.frauds == sum(e['transaction']['isFraud'] for e in expected)
    assert manifest['counts'] == {'dim_transaction_type': len({e['transaction']['type'] for e in expected}),
                                  'dim_simulation_time': len({e['transaction']['step'] for e in expected}),
                                  'dim_account': len({e['transaction'][k] for e in expected for k in ('nameOrig', 'nameDest')}),
                                  'fact_transaction': args.max_messages}
    print(f'PASS: {args.max_messages} événements Silver comparés champ par champ au CSV ; agrégats Gold globaux/type/step et faits étoile rapprochés.', flush=True)
finally:
    spark.stop()
