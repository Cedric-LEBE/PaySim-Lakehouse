"""Inspect banking events and commit offsets only after successful output."""
import argparse
import json
import math
import sys
import time
from banking.producer import event


def decode(payload):
    item = json.loads(payload)
    if not isinstance(item, dict) or type(item.get('schema_version')) is not int or item['schema_version'] != 1:
        raise ValueError('schema_version invalide')
    dataset = item.get('dataset_id')
    number = item.get('source_row')
    if not isinstance(dataset, str) or not dataset.strip() or type(number) is not int or number < 1:
        raise ValueError('Identité source invalide')
    row = item.get('transaction')
    if not isinstance(row, dict):
        raise ValueError('transaction invalide')
    # The version-1 producer emits decimal strings and integer step/labels.
    if any(type(row.get(k)) is not int for k in ['step', 'isFraud', 'isFlaggedFraud']):
        raise ValueError('Types entiers invalides')
    if any(not isinstance(v, str) for k, v in row.items() if k not in ['step', 'isFraud', 'isFlaggedFraud']):
        raise ValueError('Types texte invalides')
    expected = event(row, dataset, number)
    if item.get('event_id') != expected['event_id']:
        raise ValueError('event_id incohérent')
    return item


def process(consumer, message, output):
    item = decode(message.value())
    key = message.key()
    if key is None or key.decode('utf-8') != item['transaction']['nameOrig']:
        raise ValueError('Clé Kafka incohérente avec nameOrig')
    print(json.dumps({'topic': message.topic(), 'partition': message.partition(),
                      'offset': message.offset(), 'event': item}, allow_nan=False), file=output, flush=True)
    consumer.commit(message=message, asynchronous=False)


def main():
    from confluent_kafka import Consumer, KafkaException
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bootstrap-servers', default='localhost:9092')
    parser.add_argument('--topic', default='banking.transactions.raw')
    parser.add_argument('--group-id', default='banking-inspector-v1')
    parser.add_argument('--max-messages', type=int, default=100)
    parser.add_argument('--idle-timeout', type=float, default=20)
    args = parser.parse_args()
    if args.max_messages <= 0 or not math.isfinite(args.idle_timeout) or args.idle_timeout <= 0 or not args.group_id.strip():
        parser.error('Limites positives et group-id non vide requis')
    consumer = Consumer({'bootstrap.servers': args.bootstrap_servers, 'group.id': args.group_id,
                         'broker.address.family': 'v4', 'auto.offset.reset': 'earliest',
                         'enable.auto.commit': False, 'enable.auto.offset.store': False,
                         'queued.max.messages.kbytes': 8192})
    count = 0
    assigned = False
    last = time.monotonic()
    def on_assign(client, partitions):
        nonlocal assigned, last
        assigned = True
        last = time.monotonic()
    try:
        metadata = consumer.list_topics(timeout=10)
        if args.topic not in metadata.topics or metadata.topics[args.topic].error:
            raise ValueError('Topic absent ou inaccessible')
        consumer.subscribe([args.topic], on_assign=on_assign)
        started = time.monotonic()
        while count < args.max_messages:
            msg = consumer.poll(1)
            if msg is None:
                if not assigned and time.monotonic() - started > 60:
                    raise TimeoutError('Aucune partition attribuée après 60 secondes')
                if assigned and time.monotonic() - last >= args.idle_timeout:
                    break
                continue
            if msg.error():
                raise KafkaException(msg.error())
            process(consumer, msg, sys.stdout)
            count += 1
            last = time.monotonic()
        print(f'PASS: {count} événements lus, validés et offsets confirmés.', file=sys.stderr)
        return 0
    except KeyboardInterrupt:
        print(f'Arrêt demandé : {count} événements traités.', file=sys.stderr)
        return 130
    except Exception as exc:
        print(f'ERREUR: {exc}. Le message en échec ne sera pas volontairement validé.', file=sys.stderr)
        return 1
    finally:
        consumer.close()


if __name__ == '__main__':
    sys.exit(main())
