"""Incremental facts equal rebuilds; dimensions retain observed keys across withdrawals."""
import json
from pathlib import Path
import uuid
from unittest.mock import patch
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from banking.bronze import session
from banking.producer import event, records
from banking.silver import DATASET, MONEY
from banking.star import build as batch, register_views, TABLE_KEYS
from banking.star_incremental import build

root = Path('data/integration/star-incremental-' + uuid.uuid4().hex).resolve()
source, output = root / 'silver', root / 'star'
spark = session()
spark.sparkContext.setLogLevel('WARN')
base = next(records(Path('tests/fixtures/paysim_synthetic.csv'), DATASET, 1))['transaction']


def append(number, **changes):
    e = event({**base, **changes}, DATASET, number)
    frame = spark.createDataFrame([{**e['transaction'], 'event_id': e['event_id'], 'dataset_id': DATASET,
                                   'source_row': number, 'bronze_version': 1, 'topic': 'test',
                                   'partition': 0, 'offset': number, 'kafka_timestamp': '2026-11-06 12:00:00',
                                   'ingested_at': '2026-11-06 12:00:00'}])
    for name in MONEY:
        frame = frame.withColumn(name, F.col(name).cast('decimal(20,2)'))
    for name in ['kafka_timestamp', 'ingested_at']:
        frame = frame.withColumn(name, F.to_timestamp(name))
    frame.write.format('delta').mode('append').save(str(source))


def parity():
    manifest = batch(spark, source, root / 'reference')
    register_views(spark, output)
    for name in TABLE_KEYS:
        actual = spark.table(name).drop('silver_version')
        reference = spark.read.format('delta').load(str(root / 'reference' / name)).select(*actual.columns)
        if name == 'fact_transaction':
            assert actual.exceptAll(reference).count() == reference.exceptAll(actual).count() == 0
        else:
            assert reference.exceptAll(actual).count() == 0  # Historical extra dimensions are intentional.
    return manifest


try:
    append(1)
    first = build(spark, source, output)
    assert first['counts']['fact_transaction'] == 1
    parity()
    initial_accounts = spark.table('dim_account').select('account_key', 'account_id').collect()
    versions = first['table_versions']
    assert build(spark, source, output)['status'] == 'no_change'
    assert versions == {n: DeltaTable.forPath(spark, str(output / n)).history(1).first()['version'] for n in TABLE_KEYS}
    append(2, type='TRANSFER', step=25, nameOrig='C_NEW', nameDest='C_DEST', amount='25.17')
    assert build(spark, source, output)['counts']['fact_transaction'] == 2
    parity()
    assert set(tuple(r) for r in initial_accounts).issubset(set(tuple(r) for r in spark.table('dim_account').select('account_key', 'account_id').collect()))
    DeltaTable.forPath(spark, str(source)).update('source_row = 2', {'step': '49', 'nameDest': "'C_CHANGED'", 'amount': '26.19'})
    corrected = build(spark, source, output)
    assert corrected['counts']['dim_account'] == 5 and corrected['counts']['dim_simulation_time'] == 3
    parity()
    # Publish failure leaves the previous versions readable despite physical table MERGEs.
    previous = json.loads((output / '_snapshot.json').read_text())
    DeltaTable.forPath(spark, str(source)).delete('source_row = 2')
    import banking.star_incremental as module
    original_publish = module.publish
    def interrupted(path, state):
        if path.name == '_snapshot.json':
            raise RuntimeError('simulated interruption before manifest publication')
        return original_publish(path, state)
    with patch.object(module, 'publish', side_effect=interrupted):
        try:
            build(spark, source, output)
        except RuntimeError:
            pass
        else:
            raise AssertionError('Interruption non déclenchée')
    assert json.loads((output / '_snapshot.json').read_text()) == previous
    register_views(spark, output)
    assert spark.table('fact_transaction').count() == 2
    append(3, nameOrig='C_LATER', nameDest='M_LATER', step=73)
    resumed = build(spark, source, output)
    assert resumed['counts']['fact_transaction'] == 1  # Recovery pinned before the new arrival.
    assert resumed['counts']['dim_account'] == 5
    assert build(spark, source, output)['counts']['fact_transaction'] == 2
    parity()
    # Empty facts retain every dimension already observed.
    retained = json.loads((output / '_snapshot.json').read_text())['counts']
    DeltaTable.forPath(spark, str(source)).delete()
    empty = build(spark, source, output)
    assert empty['counts']['fact_transaction'] == 0
    assert all(empty['counts'][n] == retained[n] for n in TABLE_KEYS if n != 'fact_transaction')
    parity()
    print('PASS: Étoile incrémentale : parité des faits, clés stables, dimensions conservées, corrections, retraits, vide, relance et reprise avec ancien manifeste lisible.', flush=True)
finally:
    spark.stop()
