"""Isolated CSV cursor + real Spark/Delta: bootstrap, partial delivery, retry, no-op.

The orchestration adapter replaces shell/Kafka stages with raw Bronze writes; real
producer ranges and all three incremental builders run against temporary tables.
"""
import csv
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import uuid
from unittest.mock import patch

from pyspark.sql import functions as F
from banking import pipeline, replay
from banking.bronze import bronze_frame, session
from banking.producer import records
from banking.silver_incremental import build as silver
from banking.gold_incremental import build as gold
from banking.star_incremental import build as star
from banking.star import register_views

root = Path('data/integration/replay-resume-' + uuid.uuid4().hex).resolve()
root.mkdir(parents=True)
source = root / 'source.csv'
with Path('tests/fixtures/paysim_synthetic.csv').open() as stream:
    reader = csv.DictReader(stream)
    fields, rows = reader.fieldnames, list(reader)
rows += [{**rows[0], 'amount': '0.33', 'nameOrig': 'C_NEW_3'},
         {**rows[0], 'amount': '0.44', 'nameOrig': 'C_NEW_4'}]
with source.open('w', newline='') as stream:
    writer = csv.DictWriter(stream, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
dataset = 'resume-synthetic-v1'
bronze = root / 'data/bronze/transactions'
clean = root / 'data/silver'
aggregates = root / 'data/gold'
model = aggregates / 'star'
checkpoint = root / 'data/checkpoints/bronze-transactions/banking-source.json'
checkpoint.parent.mkdir(parents=True)
checkpoint.write_text('{}')  # No Kafka connection in this test.
cursor = root / 'data/checkpoints/csv-replay.json'
spark = session()
spark.sparkContext.setLogLevel('WARN')
offset = 0
ranges = []
fail_once = True


def append(items):
    global offset
    path = root / f'input-{offset}.json'
    with path.open('w') as stream:
        for item in items:
            stream.write(json.dumps({'key': item['transaction']['nameOrig'], 'value': json.dumps(item),
                                    'topic': 'resume.synthetic', 'partition': 0, 'offset': offset,
                                    'timestamp': '2026-01-01T00:00:00Z', 'timestampType': 0}) + '\n')
            offset += 1
    raw = (spark.read.json(str(path)).withColumn('key', F.col('key').cast('binary'))
           .withColumn('value', F.col('value').cast('binary')).withColumn('timestamp', F.to_timestamp('timestamp')))
    bronze_frame(raw).write.format('delta').mode('append').save(str(bronze))


def build():
    silver(spark, bronze, clean, dataset)
    gold(spark, clean / 'transactions', aggregates)
    star(spark, clean / 'transactions', model)


def verify(csv_path, through, directory):
    expected = list(records(csv_path, dataset, through))
    actual = spark.read.format('delta').load(str(clean / 'transactions')).orderBy('source_row').collect()
    assert len(actual) == len(expected) == through
    for row, item in zip(actual, expected):
        assert row.event_id == item['event_id'] and row.source_row == item['source_row']
        assert row.amount == Decimal(item['transaction']['amount'])
    register_views(spark, model)
    facts = spark.table('fact_transaction')
    assert facts.count() == facts.select('transaction_key').distinct().count() == through
    assert facts.agg(F.sum('amount')).first()[0] == sum(Decimal(i['transaction']['amount']) for i in expected)


def run_range(csv_path, limit, rate, offsets, directory, start_row=None):
    global fail_once
    ranges.append((start_row, limit))
    items = list(records(csv_path, dataset, limit - start_row + 1, start_row))
    assert len(items) == limit - start_row + 1
    append(items)
    if fail_once:
        fail_once = False
        return 1  # Delivered to Bronze, but no downstream validation/cursor commit.
    build()
    verify(csv_path, limit, directory)
    return 0


try:
    append(list(records(source, dataset, 2)))
    build()
    with patch.object(pipeline, 'ROOT', root), patch.object(pipeline, 'DATASET', dataset), \
         patch.object(pipeline, 'SHA256', hashlib.sha256(source.read_bytes()).hexdigest()), \
         patch.object(pipeline, 'run_pipeline', side_effect=run_range), patch.object(replay, 'verify', side_effect=verify):
        assert replay.resume(source, 4, 100, 2, 1, root / 'runs/first') == 1
        assert json.loads(cursor.read_text())['validated_through'] == 2
        assert json.loads(cursor.read_text())['pending'] == {'start_row': 3, 'end_row': 3}
        assert replay.resume(source, 4, 100, 2, 1, root / 'runs/retry') == 0
        assert json.loads(cursor.read_text())['validated_through'] == 4
        assert replay.resume(source, 4, 100, 2, 1, root / 'runs/noop') == 0
    assert ranges == [(3, 3), (3, 3), (4, 4)]
    assert spark.read.format('delta').load(str(bronze)).count() == 5
    assert spark.read.format('delta').load(str(clean / 'quarantine')).count() == 0
    verify(source, 4, root)
    print('PASS: reprise CSV 2→3 interrompu→3 rejoué→4, 5 lignes Bronze, 4 faits uniques, montants exacts et relance sans publication.', flush=True)
finally:
    spark.stop()
