"""Check quality routing, precision, conflicting identities and repeatable snapshots."""
import json
import uuid
from pathlib import Path
from pyspark.sql import functions as F
from banking.bronze import bronze_frame, session
from banking.producer import event, records
from banking.silver import DATASET, build, exact_money

root = Path('data/integration/silver-' + uuid.uuid4().hex).resolve()
root.mkdir(parents=True)
spark = session()
spark.sparkContext.setLogLevel('WARN')
base = next(records(Path('tests/fixtures/paysim_synthetic.csv'), DATASET, 1))['transaction']

def make(number, **changes):
    return event({**base, **changes}, DATASET, number)

payloads = [json.dumps(make(1)), json.dumps(make(1)),
            json.dumps(make(2, amount='0.001')), json.dumps(make(3)),
            json.dumps(make(3, amount='20.25')),
            json.dumps(event(base, 'synthetic-v1', 1)), '{bad-json']

def append(payloads, offset, mode):
    path = root / f'input-{offset}.json'
    with path.open('w') as stream:
        for i, payload in enumerate(payloads, offset):
            stream.write(json.dumps({'key': base['nameOrig'], 'value': payload,
                                     'topic': 'test', 'partition': 0, 'offset': i,
                                     'timestamp': '2026-11-06T12:00:00Z', 'timestampType': 0}) + '\n')
    raw = (spark.read.json(str(path)).withColumn('key', F.col('key').cast('binary'))
           .withColumn('value', F.col('value').cast('binary'))
           .withColumn('timestamp', F.to_timestamp('timestamp')))
    bronze_frame(raw).write.format('delta').mode(mode).save(str(root / 'bronze'))

try:
    # Scientific notation must preserve cents, never silently round or underflow.
    cases = [('1.0E+7', True), ('1.234E+2', True), ('1E-2', True),
             ('1000E-3', True), ('1.2300', True), ('0E-50', True),
             ('1.001E+0', False), ('1E-3', False), ('1E-50', False),
             ('1E+19', False), ('NaN', False), ('Infinity', False),
             ('-1', False), ('1E+9223372036854775807', False),
             ('1E-9223372036854775808', False)]
    checks = spark.createDataFrame(cases, ['raw', 'expected'])
    checks = checks.withColumn('money', F.col('raw').try_cast('decimal(20,2)'))
    checks = checks.withColumn('accepted', F.coalesce(exact_money(F.col('raw'), F.col('money')), F.lit(False)))
    assert checks.filter(F.col('accepted') != F.col('expected')).count() == 0
    append(payloads, 0, 'error')
    first = build(spark, root / 'bronze', root / 'silver')
    assert first['silver_rows'] == 1 and first['quarantine_rows'] == 5 and first['duplicates_removed'] == 1
    clean = spark.read.format('delta').load(str(root / 'silver/transactions'))
    assert str(clean.select('amount').first()[0]) == '10.25'
    assert all(str(clean.schema[name].dataType) == 'DecimalType(20,2)' for name in
               ['amount', 'oldbalanceOrg', 'newbalanceOrig', 'oldbalanceDest', 'newbalanceDest'])
    rejected = spark.read.format('delta').load(str(root / 'silver/quarantine'))
    assert rejected.filter(F.array_contains('quality_reasons', 'conflicting_event_id')).count() == 2
    assert rejected.filter(F.array_contains('quality_reasons', 'invalid_amount')).count() == 2  # bad precision + malformed JSON
    second = build(spark, root / 'bronze', root / 'silver')
    assert first == second
    append([json.dumps(make(4))], 7, 'append')
    third = build(spark, root / 'bronze', root / 'silver')
    assert third['silver_rows'] == 2 and third['quarantine_rows'] == 5
    try:
        build(spark, root / 'bronze', root / 'bronze/nested')
    except ValueError:
        pass
    else:
        raise AssertionError('Destination imbriquée acceptée')
    print('PASS: Silver 1→1→2, précision, conflits, quarantaine et déduplication vérifiés.', flush=True)
finally:
    spark.stop()
