"""Verify CDF replay, cross-commit conflicts and recovery before cursor publication."""
import json
from pathlib import Path
import uuid
from unittest.mock import patch
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from banking.bronze import bronze_frame, session
from banking.producer import event, records
from banking.silver import DATASET, build as batch
from banking.silver_incremental import build, ensure_published

root = Path('data/integration/incremental-' + uuid.uuid4().hex).resolve()
root.mkdir(parents=True)
spark = session()
spark.sparkContext.setLogLevel('WARN')
base = next(records(Path('tests/fixtures/paysim_synthetic.csv'), DATASET, 1))['transaction']
source, output = root / 'bronze', root / 'silver'


def append(items, offset):
    path = root / f'input-{offset}.json'
    with path.open('w') as stream:
        for i, item in enumerate(items, offset):
            stream.write(json.dumps({'key': base['nameOrig'], 'value': json.dumps(item),
                                     'topic': 'test', 'partition': 0, 'offset': i,
                                     'timestamp': '2026-11-06T12:00:00Z', 'timestampType': 0}) + '\n')
    raw = spark.read.json(str(path)).withColumn('key', F.col('key').cast('binary')).withColumn('value', F.col('value').cast('binary')).withColumn('timestamp', F.to_timestamp('timestamp'))
    bronze_frame(raw).write.format('delta').mode('append').save(str(source))


def make(number, **changes):
    return event({**base, **changes}, DATASET, number)


def parity():
    batch(spark, source, root / 'reference')
    for name in ['transactions', 'quarantine']:
        actual = spark.read.format('delta').load(str(output / name)).drop('bronze_version')
        expected = spark.read.format('delta').load(str(root / 'reference' / name)).drop('bronze_version')
        assert actual.exceptAll(expected.select(*actual.columns)).count() == 0, name
        assert expected.select(*actual.columns).exceptAll(actual).count() == 0, name


try:
    append([make(1), make(2, amount='0.001')], 0)
    first = build(spark, source, output)
    assert first['new_bronze_rows'] == 2 and first['silver_rows'] == 1 and first['quarantine_rows'] == 1
    versions = {name: DeltaTable.forPath(spark, str(output / name)).history(1).first()['version'] for name in ['transactions', 'quarantine', '_events']}
    assert build(spark, source, output)['status'] == 'no_change'
    assert versions == {name: DeltaTable.forPath(spark, str(output / name)).history(1).first()['version'] for name in versions}
    append([make(1), make(3)], 2)
    second = build(spark, source, output)
    assert second['new_bronze_rows'] == 2 and second['silver_rows'] == 2
    parity()
    # Different per-row Bronze versions are legitimate in one Silver snapshot.
    from banking.gold import build as gold
    from banking.star import build as star
    assert gold(spark, output / 'transactions', root / 'gold')['total_transactions'] == 2
    assert star(spark, output / 'transactions', root / 'star')['counts']['fact_transaction'] == 2
    # Different content for an older identity removes the accepted row and quarantines all occurrences.
    append([make(1, amount='20.25')], 4)
    third = build(spark, source, output)
    assert third['new_bronze_rows'] == 1 and third['silver_rows'] == 1 and third['quarantine_rows'] == 4
    parity()
    # Fail after every Delta write, just before committing the cursor.
    append([make(4)], 5)
    import banking.silver_incremental as module
    original_publish = module.publish
    def interrupted(path, state):
        if state.get('bronze_version', -1) >= third['bronze_version'] and 'pending_version' not in state:
            raise RuntimeError('simulated crash before cursor publication')
        return original_publish(path, state)
    with patch.object(module, 'publish', side_effect=interrupted):
        try:
            build(spark, source, output)
        except RuntimeError:
            pass
        else:
            raise AssertionError('Interruption non déclenchée')
    try:
        ensure_published(output / 'transactions')
    except ValueError:
        pass
    else:
        raise AssertionError('Publication partielle acceptée')
    try:
        gold(spark, output / 'transactions', root / 'blocked-gold')
    except ValueError:
        assert not (root / 'blocked-gold').exists()
    else:
        raise AssertionError('Gold a accepté Silver partiellement publié')
    assert json.loads((output / '_incremental.json').read_text())['bronze_version'] == third['bronze_version']
    resumed = build(spark, source, output)
    assert resumed['new_bronze_rows'] == 1 and resumed['silver_rows'] == 2 and resumed['quarantine_rows'] == 4
    parity()
    try:
        build(spark, source, output, dataset='another-dataset')
    except ValueError:
        pass
    else:
        raise AssertionError('Dataset différent accepté')
    # Crash between MERGEs; new arrivals during recovery wait for the next run.
    append([make(5)], 6)
    original_merge = module.merge
    def interrupted_merge(spark, path, *args, **kwargs):
        original_merge(spark, path, *args, **kwargs)
        if path.name == 'transactions':
            raise RuntimeError('simulated crash between MERGEs')
    with patch.object(module, 'merge', side_effect=interrupted_merge):
        try:
            build(spark, source, output)
        except RuntimeError:
            pass
        else:
            raise AssertionError('Interruption entre MERGEs non déclenchée')
    append([make(6)], 7)
    recovery = build(spark, source, output)
    assert recovery['new_bronze_rows'] == 1 and recovery['silver_rows'] == 3
    next_run = build(spark, source, output)
    assert next_run['new_bronze_rows'] == 1 and next_run['silver_rows'] == 4
    parity()
    hidden = output / '_events-hidden'
    (output / '_events').rename(hidden)
    try:
        try:
            build(spark, source, output)
        except ValueError as error:
            assert 'table manquante' in str(error)
        else:
            raise AssertionError('Table technique manquante acceptée')
    finally:
        hidden.rename(output / '_events')
    # Updates/deletes are outside the append-only contract and cannot advance the cursor.
    cursor = json.loads((output / '_incremental.json').read_text())['bronze_version']
    DeltaTable.forPath(spark, str(source)).delete('offset = 5')
    try:
        build(spark, source, output)
    except ValueError:
        pass
    else:
        raise AssertionError('Suppression Bronze acceptée')
    assert json.loads((output / '_incremental.json').read_text())['bronze_version'] == cursor
    print('PASS: Silver incrémental : nouvelles lignes seules, rejeu sans écriture, conflits inter-versions, parité batch, reprise et source append-only validés.', flush=True)
finally:
    spark.stop()
