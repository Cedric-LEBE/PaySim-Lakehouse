"""Reconcile a PaySim prefix without collecting all transactions in Python."""
import argparse
from collections import defaultdict
from decimal import Decimal
from itertools import zip_longest
from pathlib import Path
from banking.bronze import session
from banking.producer import records
from banking.silver import DATASET, MONEY, FIELDS
from banking.star import register_views
from pyspark.sql import functions as F

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--max-messages', type=int, default=10000)
args = parser.parse_args()
if args.max_messages <= 0:
    parser.error('max-messages doit être positif')


def empty_totals():
    return {'total_transactions': 0, 'total_amount': Decimal(0),
            'fraud_transactions': 0, 'fraud_amount': Decimal(0)}


spark = session()
spark.sparkContext.setLogLevel('WARN')
try:
    silver = spark.read.format('delta').load('data/silver/transactions')
    expected = records(Path('data/raw/PS_20174392719_1491204439457_log.csv'), DATASET, args.max_messages)
    # Only one Spark partition is transferred at a time; the CSV is streamed.
    actual = (silver.select('event_id', 'dataset_id', 'source_row', *FIELDS)
              .orderBy('source_row').toLocalIterator())
    groups = {table: defaultdict(empty_totals) for table in ('summary', 'by_type', 'by_step')}
    accounts = set()  # Distinct account IDs only, never full transaction rows.
    count = 0
    for ref, row in zip_longest(expected, actual):
        assert ref is not None and row is not None, 'Périmètre CSV/Silver différent'
        assert row.event_id == ref['event_id'] and row.dataset_id == DATASET
        assert row.source_row == ref['source_row']
        transaction = ref['transaction']
        for key, value in transaction.items():
            assert row[key] == (Decimal(value) if key in MONEY else value), (row.event_id, key)
        count += 1
        accounts.update((transaction['nameOrig'], transaction['nameDest']))
        for table, group in [('summary', None), ('by_type', transaction['type']), ('by_step', transaction['step'])]:
            totals = groups[table][group]
            totals['total_transactions'] += 1
            totals['total_amount'] += Decimal(transaction['amount'])
            totals['fraud_transactions'] += transaction['isFraud']
            if transaction['isFraud']:
                totals['fraud_amount'] += Decimal(transaction['amount'])
    assert count == args.max_messages, 'CSV plus court que le périmètre demandé'
    for table, key in [('summary', None), ('by_type', 'type'), ('by_step', 'step')]:
        output = spark.read.format('delta').load('data/gold/' + table).collect()
        assert len(output) == len(groups[table])
        for row in output:
            group = row[key] if key else None
            assert group in groups[table]
            for metric, value in groups[table][group].items():
                assert row[metric] == value, (table, group, metric)
    manifest = register_views(spark, 'data/gold/star')
    fact = spark.table('fact_transaction')
    assert fact.count() == fact.select('transaction_key').distinct().count() == count
    assert fact.select('transaction_key').subtract(silver.select('event_id')).count() == 0
    totals = fact.agg(F.sum('amount').alias('amount'), F.sum('is_fraud').alias('frauds')).first()
    assert totals.amount == groups['summary'][None]['total_amount']
    assert totals.frauds == groups['summary'][None]['fraud_transactions']
    expected_counts = {'dim_transaction_type': len(groups['by_type']),
                       'dim_simulation_time': len(groups['by_step']),
                       'dim_account': len(accounts), 'fact_transaction': count}
    assert manifest['counts'] == expected_counts
    for table, expected_count in expected_counts.items():
        assert spark.table(table).count() == expected_count, table
    print(f'PASS: {count} événements Silver comparés champ par champ au CSV ; agrégats Gold globaux/type/step et faits étoile rapprochés.', flush=True)
finally:
    spark.stop()
