"""Compare incremental Gold to full recomputation after inserts, updates and deletes."""
import json
from pathlib import Path
from decimal import Decimal
import uuid
from unittest.mock import patch
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from banking.bronze import session
from banking.gold import build as batch
from banking.gold_incremental import build, TABLES, read_tables

root = Path('data/integration/gold-incremental-' + uuid.uuid4().hex).resolve()
source, output = root / 'silver', root / 'gold'
spark = session()
spark.sparkContext.setLogLevel('WARN')


def append(rows):
    frame = spark.createDataFrame(rows, 'event_id string, type string, step long, amount string, isFraud long, isFlaggedFraud long')
    (frame.withColumn('amount', F.col('amount').cast('decimal(20,2)'))
     .withColumn('dataset_id', F.lit('fixture')).withColumn('bronze_version', F.lit(1))
     .write.format('delta').mode('append').save(str(source)))


def parity():
    expected = batch(spark, source, root / 'reference')
    for name in TABLES:
        actual = spark.read.format('delta').load(str(output / name)).drop('silver_version', 'bronze_version', 'dataset_id')
        reference = spark.read.format('delta').load(str(root / 'reference' / name)).select(*actual.columns)
        assert actual.exceptAll(reference).count() == reference.exceptAll(actual).count() == 0, name
    return expected


try:
    append([('a','PAYMENT',1,'0.11',0,0), ('b','TRANSFER',2,'10.13',1,1)])
    assert build(spark, source, output)['total_transactions'] == 2
    parity()
    versions = {n: DeltaTable.forPath(spark, str(output / n)).history(1).first()['version'] for n in TABLES}
    assert build(spark, source, output)['status'] == 'no_change'
    assert versions == {n: DeltaTable.forPath(spark, str(output / n)).history(1).first()['version'] for n in TABLES}
    append([('c','PAYMENT',1,'0.22',0,0)])
    assert build(spark, source, output)['total_transactions'] == 3
    parity()
    DeltaTable.forPath(spark, str(source)).update("event_id = 'b'", {'type': "'CASH_OUT'", 'step': '3', 'amount': '20.17', 'isFlaggedFraud': '0'})
    assert build(spark, source, output)['fraud_amount'] == Decimal('20.17')
    parity()
    # Conflict withdrawal removes its entire type/hour group.
    DeltaTable.forPath(spark, str(source)).delete("event_id = 'b'")
    import banking.gold_incremental as module
    real_merge = module.merge
    def interrupted(spark, path, *args, **kwargs):
        real_merge(spark, path, *args, **kwargs)
        if path.name == 'summary':
            raise RuntimeError('simulated crash after summary MERGE')
    cursor = json.loads((output / '_gold_incremental.json').read_text())['silver_version']
    with patch.object(module, 'merge', side_effect=interrupted):
        try:
            build(spark, source, output)
        except RuntimeError:
            pass
        else:
            raise AssertionError('Interruption non déclenchée')
    assert json.loads((output / '_gold_incremental.json').read_text())['silver_version'] == cursor
    try:
        read_tables(spark, output)
    except ValueError:
        pass
    else:
        raise AssertionError('Lecture partielle Gold acceptée')
    append([('d','DEBIT',4,'7.00',0,0)])
    assert build(spark, source, output)['total_transactions'] == 2  # Pending range only.
    assert build(spark, source, output)['total_transactions'] == 3
    parity()
    DeltaTable.forPath(spark, str(source)).delete()
    empty = build(spark, source, output)
    assert empty['total_transactions'] == 0 and empty['total_amount'] == 0
    assert empty['avg_amount'] is None and empty['fraud_rate'] is None
    parity()
    assert spark.read.format('delta').load(str(output / 'by_type')).count() == 0
    append([('e','TRANSFER',5,'1.00',1,0)])
    assert build(spark, source, output)['total_transactions'] == 1
    parity()
    print('PASS: Gold incrémental : parité complète, insertions, corrections, retraits, groupes disparus, vide, rejeu et reprise sans double comptage.', flush=True)
finally:
    spark.stop()
