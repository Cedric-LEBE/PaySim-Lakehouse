"""CI integration: synthetic Bronze -> incremental Silver -> Gold -> star, no Kafka."""
from decimal import Decimal
import json
from pathlib import Path
import uuid
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from banking.bronze import bronze_frame, session
from banking.producer import event, records
from banking.silver_incremental import build as silver
from banking.gold_incremental import build as gold, read_tables
from banking.star_incremental import build as star
from banking.gold import build as gold_batch
from banking.star import build as star_batch, register_views, TABLE_KEYS

DATASET = 'ci-synthetic-v1'
root = Path('data/integration/pipeline-' + uuid.uuid4().hex).resolve()
root.mkdir(parents=True)
bronze, clean, aggregates, model = [root / name for name in ('bronze', 'silver', 'gold', 'star')]
fixture = list(records(Path('tests/fixtures/paysim_synthetic.csv'), DATASET, 2))
spark = session()
spark.sparkContext.setLogLevel('WARN')


def append(items, offset):
    path = root / f'input-{offset}.json'
    with path.open('w') as stream:
        for number, item in enumerate(items, offset):
            stream.write(json.dumps({'key': item['transaction']['nameOrig'], 'value': json.dumps(item),
                                     'topic': 'ci.synthetic', 'partition': 0, 'offset': number,
                                     'timestamp': '2026-01-01T00:00:00Z', 'timestampType': 0}) + '\n')
    raw = (spark.read.json(str(path)).withColumn('key', F.col('key').cast('binary'))
           .withColumn('value', F.col('value').cast('binary')).withColumn('timestamp', F.to_timestamp('timestamp')))
    bronze_frame(raw).write.format('delta').mode('append').save(str(bronze))


def check(expected_rows, expected_amount, expected_frauds):
    silver(spark, bronze, clean, DATASET)
    result = gold(spark, clean / 'transactions', aggregates)
    star(spark, clean / 'transactions', model)
    assert result['total_transactions'] == expected_rows
    assert result['total_amount'] == Decimal(expected_amount)
    assert result['fraud_transactions'] == expected_frauds
    # Independently reconstruct aggregates and facts from the resulting Silver snapshot.
    gold_batch(spark, clean / 'transactions', root / 'gold-reference')
    star_batch(spark, clean / 'transactions', root / 'star-reference')
    for name, actual in read_tables(spark, aggregates).items():
        actual = actual.drop('silver_version', 'bronze_version', 'dataset_id')
        reference = spark.read.format('delta').load(str(root / 'gold-reference' / name)).select(*actual.columns)
        assert actual.exceptAll(reference).count() == reference.exceptAll(actual).count() == 0, name
    register_views(spark, model)
    actual = spark.table('fact_transaction').drop('silver_version')
    reference = spark.read.format('delta').load(str(root / 'star-reference/fact_transaction')).select(*actual.columns)
    assert actual.exceptAll(reference).count() == reference.exceptAll(actual).count() == 0
    assert spark.table('fact_transaction').count() == expected_rows
    # All four foreign keys must resolve, including both roles of the account dimension.
    joined = spark.sql('''SELECT f.transaction_key FROM fact_transaction f
      JOIN dim_transaction_type ty ON f.transaction_type_key = ty.transaction_type_key
      JOIN dim_simulation_time tm ON f.simulation_time_key = tm.simulation_time_key
      JOIN dim_account orig ON f.origin_account_key = orig.account_key
      JOIN dim_account dest ON f.destination_account_key = dest.account_key''')
    assert joined.count() == expected_rows


def versions():
    paths = [clean / name for name in ('transactions', 'quarantine', '_events')]
    paths += [aggregates / name for name in ('summary', 'by_type', 'by_step')]
    paths += [model / name for name in TABLE_KEYS]
    return {str(path): DeltaTable.forPath(spark, str(path)).history(1).first()['version'] for path in paths}


try:
    append(fixture, 0)
    check(2, '60.25', 1)
    before, manifest = versions(), (model / '_snapshot.json').read_text()
    assert silver(spark, bronze, clean, DATASET)['status'] == 'no_change'
    assert gold(spark, clean / 'transactions', aggregates)['status'] == 'no_change'
    assert star(spark, clean / 'transactions', model)['status'] == 'no_change'
    assert versions() == before and (model / '_snapshot.json').read_text() == manifest
    third = event({**fixture[0]['transaction'], 'type': 'CASH_IN', 'step': '25',
                   'amount': '0.33', 'nameOrig': 'C_NEW', 'nameDest': 'C_NEW'}, DATASET, 3)
    append([fixture[0], third], 2)  # Business replay plus a genuinely new event.
    check(3, '60.58', 1)
    assert spark.read.format('delta').load(str(clean / 'transactions')).select('event_id').distinct().count() == 3
    conflict = event({**fixture[1]['transaction'], 'amount': '51.00'}, DATASET, 2)
    append([conflict], 4)
    check(2, '10.58', 0)
    rejected = spark.read.format('delta').load(str(clean / 'quarantine'))
    assert rejected.count() == rejected.filter(F.array_contains('quality_reasons', 'conflicting_event_id')).count() == 2
    assert spark.table('dim_transaction_type').count() == 3  # Withdrawn TRANSFER remains historical.
    self_transfer = spark.table('fact_transaction').filter(F.col('transaction_key') == third['event_id']).first()
    assert self_transfer.origin_account_key == self_transfer.destination_account_key
    print('PASS: CI pipeline synthétique : 2→3→2 faits, rejeu sans écriture, déduplication, conflit retiré, parité batch et quatre références validés.', flush=True)
finally:
    spark.stop()
