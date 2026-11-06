"""Run and verify a bounded local PaySim replay, one stage at a time."""
import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

from banking.producer import records
from banking.silver import DATASET

ROOT = Path(__file__).resolve().parents[2]
CSV = Path('data/raw/PS_20174392719_1491204439457_log.csv')
SHA256 = DATASET.removeprefix('paysim-sha256-')


def preflight(csv, limit):
    """Validate the exact source and requested prefix before any publication."""
    with csv.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    if digest != SHA256:
        raise ValueError('Empreinte CSV incorrecte : aucun événement publié')
    count = sum(1 for _ in records(csv, DATASET, limit))
    if count != limit:
        raise ValueError(f'CSV trop court : {count} lignes disponibles pour {limit} demandées')
    manifest = ROOT / 'data/gold/star/_snapshot.json'
    if manifest.exists() and json.loads(manifest.read_text())['counts']['fact_transaction'] > limit:
        raise ValueError('Volume inférieur à l’étoile publiée : choisir un volume égal ou supérieur')


def execute(command, log, env):
    with log.open('w') as output:
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=output,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        try:
            return process.wait()
        except BaseException:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            raise


def run_pipeline(csv, limit, rate, batch, directory):
    directory.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    report = {'status': 'running', 'max_messages': limit, 'rate': rate,
              'max_offsets': batch, 'csv': str(csv), 'stages': []}
    env = {**os.environ, 'PYTHONPATH': str(ROOT / 'src')}
    # Select every Spark application explicitly, irrespective of the calling shell.
    env.pop('BANKING_SPARK_APP', None)
    kafka_active = False

    def stage(name, command):
        entry = {'name': name, 'log': str(directory / f'{name}.log')}
        report['stages'].append(entry)
        begin = time.monotonic()
        print(f'→ {name}', flush=True)
        try:
            code = execute(command, Path(entry['log']), env)
            entry['exit_code'] = code
            if code:
                tail = Path(entry['log']).read_text(errors='replace').splitlines()[-20:]
                raise RuntimeError(f'{name} a échoué (code {code}).\n' + '\n'.join(tail))
        finally:
            entry['seconds'] = round(time.monotonic() - begin, 2)
        print(f'PASS: {name} ({entry["seconds"]:.2f} s)', flush=True)

    try:
        begin = time.monotonic()
        entry = {'name': 'preflight', 'exit_code': 1}
        report['stages'].append(entry)
        try:
            preflight(csv, limit)
            entry['exit_code'] = 0
        finally:
            entry['seconds'] = round(time.monotonic() - begin, 2)
        print('PASS: CSV vérifié et préfixe validé.', flush=True)
        kafka_active = True  # Also clean up a partially successful start.
        stage('kafka-start', ['bash', 'scripts/kafka.sh', 'start'])
        stage('kafka-topic', ['bash', 'scripts/kafka.sh', 'topic'])
        stage('producer', [str(ROOT / '.venv/bin/python'), '-m', 'banking.producer',
                          '--csv', str(csv), '--dataset-id', DATASET,
                          '--max-messages', str(limit), '--rate', str(rate)])
        stage('bronze', ['bash', 'scripts/bronze.sh', '--max-offsets', str(batch)])
        stage('kafka-stop', ['bash', 'scripts/kafka.sh', 'stop'])
        kafka_active = False
        for layer in ('silver', 'gold', 'star'):
            stage(layer, ['bash', f'scripts/{layer}.sh'])
        stage('verify', ['env', 'BANKING_SPARK_APP=tests/integration/replay_check.py',
                         'bash', 'scripts/bronze.sh', '--max-messages', str(limit)])
        report['status'] = 'passed'
    except (Exception, KeyboardInterrupt) as error:
        report['status'] = 'failed'
        report['error'] = str(error) or 'Exécution interrompue'
        print('ERREUR: ' + report['error'], file=sys.stderr, flush=True)
    finally:
        if kafka_active:
            try:
                stage('kafka-cleanup', ['bash', 'scripts/kafka.sh', 'stop'])
            except (Exception, KeyboardInterrupt) as error:
                report['status'] = 'failed'
                report['cleanup_error'] = str(error) or 'Nettoyage interrompu'
                print('ERREUR: arrêt Kafka impossible ; consulter kafka-cleanup.log.', file=sys.stderr)
        report['seconds'] = round(time.monotonic() - started, 2)
        (directory / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
        print(f'Bilan : {directory / "report.json"}', flush=True)
    return 0 if report['status'] == 'passed' else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--max-messages', type=int, default=10000)
    parser.add_argument('--rate', type=float, default=100)
    parser.add_argument('--max-offsets', type=int, default=1000)
    parser.add_argument('--csv', type=Path, default=CSV)
    args = parser.parse_args()
    import math
    if args.max_messages <= 0 or args.max_offsets <= 0 or not math.isfinite(args.rate) or args.rate <= 0:
        parser.error('max-messages, max-offsets et rate doivent être positifs et finis')
    csv = (ROOT / args.csv).resolve()
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:8]
    runs = ROOT / 'data/runs'
    runs.mkdir(parents=True, exist_ok=True)
    with (runs / 'pipeline.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print('ERREUR: un pipeline est déjà en cours.', file=sys.stderr)
            return 1
        return run_pipeline(csv, args.max_messages, args.rate, args.max_offsets, runs / run_id)


def interrupted(*_):
    raise KeyboardInterrupt


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, interrupted)
    sys.exit(main())
