"""Reconcile Gold with the original CSV, plus zero-data and duplicate-input checks."""
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
import uuid
from pyspark.sql import functions as F
from banking.bronze import session
from banking.gold import build, metrics
from banking.producer import records
from banking.silver import DATASET

root = Path('data/integration/gold-' + uuid.uuid4().hex).resolve()
spark = session()
spark.sparkContext.setLogLevel('WARN')

try:
    expected = list(records(Path('data/raw/PS_20174392719_1491204439457_log.csv'), DATASET, 100))
    silver = spark.read.format('delta').load(str(Path('data/silver/transactions').resolve()))
    assert {r[0] for r in silver.select('event_id').collect()} == {r['event_id'] for r in expected}, 'Silver doit contenir les 100 premières transactions PaySim pour ce test'
    first = build(spark, 'data/silver/transactions', root / 'gold')
    amount = sum((Decimal(r['transaction']['amount']) for r in expected), Decimal('0.00'))
    frauds = sum(r['transaction']['isFraud'] for r in expected)
    fraud_amount = sum((Decimal(r['transaction']['amount']) for r in expected if r['transaction']['isFraud']), Decimal('0.00'))
    assert first['total_transactions'] == 100
    assert first['total_amount'] == amount and first['fraud_transactions'] == frauds
    assert first['fraud_amount'] == fraud_amount
    assert first['fraud_rate'] == Decimal(frauds) / Decimal(100)
    assert first['avg_amount'] == (amount / 100).quantize(Decimal('0.000001'))
    assert first['flagged_transactions'] == sum(r['transaction']['isFlaggedFraud'] for r in expected)
    for table, key in [('by_type', 'type'), ('by_step', 'step')]:
        grouped = defaultdict(list)
        for row in expected: grouped[row['transaction'][key]].append(row['transaction'])
        actual = spark.read.format('delta').load(str(root / 'gold' / table)).collect()
        assert {row[key] for row in actual} == set(grouped)
        for row in actual:
            source = grouped[row[key]]
            assert row['total_transactions'] == len(source)
            assert row['total_amount'] == sum((Decimal(x['amount']) for x in source), Decimal('0'))
            assert row['fraud_transactions'] == sum(x['isFraud'] for x in source)
            assert row['fraud_amount'] == sum((Decimal(x['amount']) for x in source if x['isFraud']), Decimal('0'))
    assert build(spark, 'data/silver/transactions', root / 'gold') == first
    sample = spark.range(2).select(F.when(F.col('id') == 0, '0.10').otherwise('0.20').cast('decimal(20,2)').alias('amount'),
                                  F.lit(0).alias('isFraud'), F.when(F.col('id') == 0, 1).otherwise(0).alias('isFlaggedFraud'))
    no_fraud = metrics(sample).first()
    assert no_fraud.total_amount == Decimal('0.30') and no_fraud.fraud_amount == 0 and no_fraud.fraud_rate == 0
    assert no_fraud.flagged_transactions == 1 and no_fraud.fraud_transactions == 0
    empty = metrics(sample.limit(0)).first()
    assert empty.total_transactions == 0 and empty.total_amount == 0 and empty.fraud_amount == 0
    assert empty.avg_amount is None and empty.fraud_rate is None
    # Invalid source must fail before writing metrics.
    silver.unionByName(silver.limit(1)).write.format('delta').save(str(root / 'duplicated-silver'))
    try:
        build(spark, root / 'duplicated-silver', root / 'invalid-gold')
    except ValueError:
        assert not (root / 'invalid-gold').exists()
    else:
        raise AssertionError('Silver dupliquée acceptée')
    try:
        build(spark, 'data/silver/transactions', 'data/silver/transactions/nested')
    except ValueError:
        pass
    else:
        raise AssertionError('Destination imbriquée acceptée')
    print('PASS: Gold rapprochée du CSV (global/type/step), relance stable, décimales, cas vide/sans fraude et doublons vérifiés.', flush=True)
finally:
    spark.stop()
