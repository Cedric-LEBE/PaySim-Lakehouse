"""Run against local Kafka; temporary topic and bounded real-topic inspection."""
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from banking.producer import records

config = {'bootstrap.servers': 'localhost:9092', 'broker.address.family': 'v4'}
admin = AdminClient(config)
topic = 'banking.consumer.check.' + uuid.uuid4().hex
admin.create_topics([NewTopic(topic, 1, 1)])[topic].result(20)
env = {**os.environ, 'PYTHONPATH': 'src'}

def consume(topic, group, maximum):
    result = subprocess.run([sys.executable, '-m', 'banking.consumer', '--topic', topic,
                             '--group-id', group, '--max-messages', str(maximum), '--idle-timeout', '5'],
                            env=env, capture_output=True, text=True, timeout=90)
    if result.returncode:
        raise RuntimeError(result.stderr)
    return [json.loads(line) for line in result.stdout.splitlines()]

try:
    producer = Producer(config)
    for item in records(Path('tests/fixtures/paysim_synthetic.csv'), 'integration-synthetic', 2):
        producer.produce(topic, key=item['transaction']['nameOrig'], value=json.dumps(item))
    assert producer.flush(20) == 0
    first = consume(topic, topic, 1)
    second = consume(topic, topic, 1)
    third = consume(topic, topic, 1)
    assert [first[0]['offset'], second[0]['offset']] == [0, 1]
    assert not third
    print('PASS: reprise du même groupe aux offsets 0 puis 1, puis aucun rejeu.')
    actual = consume('banking.transactions.raw', 'banking-validation-' + uuid.uuid4().hex, 200)
    real = [x for x in actual if x['event']['dataset_id'].startswith('paysim-sha256-')]
    assert real, 'Aucune transaction du CSV réel trouvée'
    print(f'PASS: {len(actual)} événements validés, dont {len(real)} PaySim réels ; partitions={sorted({x["partition"] for x in real})}.')
finally:
    admin.delete_topics([topic])[topic].result(20)
