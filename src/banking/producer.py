"""Replay a PaySim CSV with bounded memory and explicit delivery confirmation."""
import argparse
import csv
import hashlib
import json
import math
import sys
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path

COLUMNS = 'step type amount nameOrig oldbalanceOrg newbalanceOrig nameDest oldbalanceDest newbalanceDest isFraud isFlaggedFraud'.split()
MONEY = ['amount', 'oldbalanceOrg', 'newbalanceOrig', 'oldbalanceDest', 'newbalanceDest']


def event(row, dataset, number):
    if set(row) != set(COLUMNS) or any(v is None or v == '' for v in row.values()):
        raise ValueError(f'ligne {number}: colonnes manquantes, supplémentaires ou vides')
    data = dict(row)
    try:
        for key in MONEY:
            value = Decimal(row[key])
            if not value.is_finite() or value < 0:
                raise ValueError(f'{key}: montant invalide')
            data[key] = str(value)  # Preserve decimal precision in the JSON contract.
        for key in ['step', 'isFraud', 'isFlaggedFraud']:
            data[key] = int(row[key])
        if data['step'] < 0 or any(data[k] not in (0, 1) for k in ['isFraud', 'isFlaggedFraud']):
            raise ValueError('step ou label invalide')
        if data['type'] not in {'PAYMENT', 'TRANSFER', 'CASH_OUT', 'CASH_IN', 'DEBIT'}:
            raise ValueError('type inconnu')
    except (ValueError, InvalidOperation) as exc:
        raise ValueError(f'ligne {number}: {exc}') from exc
    return {'schema_version': 1, 'event_id': hashlib.sha256(f'{dataset}:{number}'.encode()).hexdigest(),
            'dataset_id': dataset, 'source_row': number, 'transaction': data}


def records(path, dataset, limit, start_row=1):
    """Yield up to limit events, preserving absolute CSV row numbers on resume."""
    if start_row < 1 or limit < 1:
        raise ValueError('start_row et limit doivent être positifs')
    with path.open(newline='', encoding='utf-8-sig') as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames is None or len(reader.fieldnames) != len(COLUMNS) or set(reader.fieldnames) != set(COLUMNS):
            raise ValueError('En-tête CSV incompatible avec PaySim')
        for number, row in enumerate(reader, 1):
            if number < start_row:
                continue
            if number >= start_row + limit:
                break
            yield event(row, dataset, number)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--csv', type=Path, required=True)
    parser.add_argument('--dataset-id', required=True, help='Identifiant stable de cette version exacte du CSV')
    parser.add_argument('--bootstrap-servers', default='localhost:9092')
    parser.add_argument('--topic', default='banking.transactions.raw')
    parser.add_argument('--rate', type=float, default=10)
    parser.add_argument('--max-messages', type=int, default=100)
    parser.add_argument('--start-row', type=int, default=1, help='Première ligne de données, numérotation à partir de 1')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    if not math.isfinite(args.rate) or args.rate <= 0 or args.max_messages <= 0 or args.start_row <= 0 or not args.dataset_id.strip():
        parser.error('rate, max-messages et start-row doivent être positifs ; dataset-id non vide')
    producer = None
    delivered = 0
    failures = []
    def acknowledged(error, message):
        nonlocal delivered
        if error:
            if len(failures) < 5:
                failures.append(str(error))
        else:
            delivered += 1
    try:
        if not args.dry_run:
            from confluent_kafka import Producer
            producer = Producer({'bootstrap.servers': args.bootstrap_servers, 'enable.idempotence': True, 'broker.address.family': 'v4',
                                 'acks': 'all', 'queue.buffering.max.kbytes': 8192,
                                 'delivery.timeout.ms': 15000, 'request.timeout.ms': 5000})
            metadata = producer.list_topics(timeout=10)
            if args.topic not in metadata.topics or metadata.topics[args.topic].error:
                raise ValueError(f'Topic absent ou inaccessible : {args.topic}')
        count = 0
        next_send = time.monotonic()
        for item in records(args.csv, args.dataset_id, args.max_messages, args.start_row):
            payload = json.dumps(item, allow_nan=False)
            if args.dry_run:
                print(payload)
            else:
                time.sleep(max(0, next_send - time.monotonic()))
                producer.poll(0)
                if failures:
                    raise RuntimeError(failures[0])
                producer.produce(args.topic, key=item['transaction']['nameOrig'], value=payload, on_delivery=acknowledged)
                next_send = time.monotonic() + 1 / args.rate
            count += 1
        if count != args.max_messages:
            raise ValueError(f'CSV trop court : {count}/{args.max_messages} événements lus')
        if producer:
            pending = producer.flush(20)
            if pending or failures or delivered != count:
                raise RuntimeError(f'Livraison incomplète : confirmés={delivered}/{count}, en attente={pending}, erreurs={failures}')
        print(f'PASS: {count} événements validés, {delivered} livraisons confirmées.', file=sys.stderr)
        return 0
    except (Exception, KeyboardInterrupt) as exc:
        print(f'ERREUR: {exc}. Un rejeu peut créer des doublons métier.', file=sys.stderr)
        return 1
    finally:
        if producer:
            producer.flush(20)


if __name__ == '__main__':
    sys.exit(main())
