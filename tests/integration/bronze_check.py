"""Verify raw retention, checkpoint restart and new data using an isolated topic."""
import json
import uuid
from pathlib import Path
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from banking.bronze import session, ingest
from banking.producer import records

run = uuid.uuid4().hex
topic = 'banking.bronze.check.' + run
root = Path('data/integration') / run
config = {'bootstrap.servers': '127.0.0.1:9092', 'broker.address.family': 'v4'}
admin = AdminClient(config)
admin.create_topics([NewTopic(topic, 1, 1)])[topic].result(30)
spark = session()
spark.sparkContext.setLogLevel('WARN')
producer = Producer(config)
errors = []
def send(value):
    producer.produce(topic, key='C_SYNTH_1', value=value,
                     on_delivery=lambda error, msg: errors.append(str(error)) if error else None)
    assert producer.flush(20) == 0 and not errors

def run_ingestion():
    ingest(spark, '127.0.0.1:9092', topic, root / 'table', root / 'checkpoint', 2)
    return spark.read.format('delta').load(str((root / 'table').resolve()))

try:
    item = next(records(Path('tests/fixtures/paysim_synthetic.csv'), 'bronze-test', 1))
    send(json.dumps(item))
    send('{not-json')
    first = run_ingestion()
    assert first.count() == 2
    assert first.filter("parse_status = 'invalid_envelope'").count() == 1
    assert first.filter("payload = '{not-json'").count() == 1
    assert str(first.filter("parse_status = 'parsed'").select('amount').first()[0]) == '10.25'
    assert run_ingestion().count() == 2
    # A business replay is a new Kafka offset and must remain in raw Bronze.
    send(json.dumps(item))
    final = run_ingestion()
    assert final.count() == 3
    assert final.select('topic', 'partition', 'offset').distinct().count() == 3
    assert final.filter("parse_status = 'parsed'").select('event.event_id').distinct().count() == 1
    try:
        ingest(spark, '127.0.0.1:9092', topic + '.wrong', root / 'table', root / 'checkpoint')
    except ValueError:
        pass
    else:
        raise AssertionError('Source changée acceptée avec le même checkpoint')
    print('PASS: JSON malformé conservé, Decimal exact, reprise 2→2→3, offsets uniques et rejeu métier conservé.', flush=True)
finally:
    spark.stop()
    admin.delete_topics([topic])[topic].result(30)
