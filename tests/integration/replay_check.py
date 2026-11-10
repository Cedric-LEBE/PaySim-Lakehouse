"""Reconcile a PaySim prefix without collecting all transactions in Python."""
import argparse
from banking.timing import measure
from collections import defaultdict
from decimal import Decimal
from itertools import zip_longest
from pathlib import Path
from banking.bronze import session
from banking.producer import records
from banking.silver import DATASET, MONEY, FIELDS
from banking.star import register_views
from banking.gold_incremental import read_tables
from pyspark.sql import functions as F

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--max-messages', type=int, default=10000)
parser.add_argument('--data-root', type=Path, default=Path('data'))
parser.add_argument('--csv', type=Path, default=Path('data/raw/PS_20174392719_1491204439457_log.csv'))
args = parser.parse_args()
data_root = args.data_root.resolve()
if args.max_messages <= 0:
    parser.error('max-messages doit être positif')


def empty_totals():
    return {'total_transactions': 0, 'total_amount': Decimal(0),
            'fraud_transactions': 0, 'fraud_amount': Decimal(0)}


spark = session()
spark.sparkContext.setLogLevel('WARN')
try:
    with measure('verify', 'compare_csv_silver_and_expected_totals'):
        silver = spark.read.format('delta').load(str(data_root / 'silver/transactions'))
        expected = records(args.csv, DATASET, args.max_messages)
        # Only one Spark partition is transferred at a time; the CSV is streamed.
        actual = (silver.select('event_id', 'dataset_id', 'source_row', *FIELDS)
                  .repartitionByRange(128, 'source_row').sortWithinPartitions('source_row').toLocalIterator())
        groups = {table: defaultdict(empty_totals) for table in ('summary', 'by_type', 'by_step')}
        count = 0
        for ref, row in zip_longest(expected, actual):
            assert ref is not None and row is not None, 'Périmètre CSV/Silver différent'
            assert row.event_id == ref['event_id'] and row.dataset_id == DATASET
            assert row.source_row == ref['source_row']
            transaction = ref['transaction']
            for key, value in transaction.items():
                assert row[key] == (Decimal(value) if key in MONEY else value), (row.event_id, key)
            count += 1
            for table, group in [('summary', None), ('by_type', transaction['type']), ('by_step', transaction['step'])]:
                totals = groups[table][group]
                totals['total_transactions'] += 1
                totals['total_amount'] += Decimal(transaction['amount'])
                totals['fraud_transactions'] += transaction['isFraud']
                if transaction['isFraud']:
                    totals['fraud_amount'] += Decimal(transaction['amount'])
        assert count == args.max_messages, 'CSV plus court que le périmètre demandé'
    with measure('verify', 'reconcile_gold'):
        gold_tables = read_tables(spark, data_root / 'gold')
        for table, key in [('summary', None), ('by_type', 'type'), ('by_step', 'step')]:
            output = gold_tables[table].collect()
            assert len(output) == len(groups[table])
            for row in output:
                group = row[key] if key else None
                assert group in groups[table]
                for metric, value in groups[table][group].items():
                    assert row[metric] == value, (table, group, metric)
    with measure('verify', 'reconcile_star_facts'):
        manifest = register_views(spark, data_root / 'gold/star')
        fact = spark.table('fact_transaction')
        assert fact.count() == fact.select('transaction_key').distinct().count() == count
        assert fact.select('transaction_key').subtract(silver.select('event_id')).count() == 0
        totals = fact.agg(F.sum('amount').alias('amount'), F.sum('is_fraud').alias('frauds')).first()
        assert totals.amount == groups['summary'][None]['total_amount']
        assert totals.frauds == groups['summary'][None]['fraud_transactions']
    with measure('verify', 'reconcile_dimensions'):
        # Silver was compared field by field with the CSV above. Keep account membership
        # checks in Spark rather than accumulating millions of account IDs in Python.
        accounts = (silver.select(F.col('nameOrig').alias('account_id'))
                    .union(silver.select(F.col('nameDest').alias('account_id'))).distinct())
        expected_counts = {'dim_transaction_type': len(groups['by_type']),
                           'dim_simulation_time': len(groups['by_step']),
                           'dim_account': accounts.count(), 'fact_transaction': count}
        assert manifest['counts']['fact_transaction'] == count
        for table, expected_count in expected_counts.items():
            actual_count = spark.table(table).count()
            assert actual_count == manifest['counts'][table], table
            assert actual_count >= expected_count, table
        # Retained historical dimension members may outnumber the current CSV prefix.
        published_accounts = spark.table('dim_account').filter(F.col('dataset_id') == DATASET).select('account_id')
        assert accounts.join(published_accounts, 'account_id', 'left_anti').limit(1).count() == 0, 'Comptes attendus absents de la dimension'
        assert set(groups['by_type']).issubset({r.transaction_type for r in spark.table('dim_transaction_type').select('transaction_type').collect()})
        assert set(groups['by_step']).issubset({r.simulation_step for r in spark.table('dim_simulation_time').filter(F.col('dataset_id') == DATASET).select('simulation_step').collect()})
    print(f'PASS: {count} événements Silver comparés champ par champ au CSV ; agrégats Gold globaux/type/step et faits étoile rapprochés.', flush=True)
finally:
    spark.stop()
