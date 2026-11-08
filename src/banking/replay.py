"""Durable CSV replay cursor; commit a range only after full lakehouse verification."""
import hashlib
import json
import os
import sys

from banking import pipeline


def save(path, value):
    """Publish the cursor atomically, including after an interrupted file write."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    with temporary.open('w') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def verify(csv, through, directory):
    directory.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, 'PYTHONPATH': str(pipeline.ROOT / 'src')}
    env.pop('BANKING_SPARK_APP', None)
    command = ['env', 'BANKING_SPARK_APP=tests/integration/replay_check.py',
               'bash', 'scripts/bronze.sh', '--max-messages', str(through), '--csv', str(csv)]
    log = directory / 'bootstrap-verify.log'
    if pipeline.execute(command, log, env):
        raise ValueError('Préfixe existant non validé : consulter ' + str(log))


def resume(csv, target, rate, max_offsets, batch_rows, directory):
    """Called with the shared pipeline flock held, never independently of that lock."""
    root = pipeline.ROOT
    cursor = root / 'data/checkpoints/csv-replay.json'
    snapshot = root / 'data/gold/star/_snapshot.json'
    checkpoint = root / 'data/checkpoints/bronze-transactions/banking-source.json'
    identity = {'format_version': 1, 'csv': str(csv.resolve()), 'dataset_id': pipeline.DATASET,
                'root': str(root.resolve())}
    try:
        if not 1 <= batch_rows <= 50000 or target < 1:
            raise ValueError('Cible positive et batch-rows entre 1 et 50000 requis')
        with csv.open('rb') as stream:
            if hashlib.file_digest(stream, 'sha256').hexdigest() != pipeline.SHA256:
                raise ValueError('Empreinte CSV incorrecte : aucun événement publié')
        if cursor.exists():
            state = json.loads(cursor.read_text())
            if any(state.get(key) != value for key, value in identity.items()):
                raise ValueError('Curseur associé à un autre CSV, dataset ou dépôt')
            through = state.get('validated_through')
            if type(through) is not int or through < 0:
                raise ValueError('Curseur CSV invalide')
            pending = state.get('pending')
            if pending is not None and (set(pending) != {'start_row', 'end_row'} or
                    type(pending['start_row']) is not int or type(pending['end_row']) is not int or
                    pending['start_row'] != through + 1 or not through < pending['end_row'] <= through + 50000):
                raise ValueError('Lot en attente invalide')
            if through and (not checkpoint.exists() or not snapshot.exists()):
                raise ValueError('Checkpoint Bronze ou instantané étoile manquant ; reprise refusée')
            if not pending and through and json.loads(snapshot.read_text()) != state.get('snapshot'):
                raise ValueError('Instantané modifié hors de cette reprise ; vérifier les tables avant de continuer')
        else:
            through = 0
            if snapshot.exists():
                published = json.loads(snapshot.read_text())
                through = published['counts']['fact_transaction']
                if type(through) is not int or through < 1 or not checkpoint.exists():
                    raise ValueError('Instantané/checkpoint existant invalide')
                if target < through:
                    raise ValueError('Cible inférieure au préfixe existant')
                print(f'→ Validation du préfixe existant : {through} lignes', flush=True)
                verify(csv, through, directory)
            elif any((root / path / '_delta_log').exists() for path in
                     ('data/bronze/transactions', 'data/silver/transactions')):
                raise ValueError('Tables existantes sans étoile publiée : terminer le pipeline avant la reprise')
            state = {**identity, 'validated_through': through, 'pending': None,
                     'snapshot': json.loads(snapshot.read_text()) if snapshot.exists() else None}
            save(cursor, state)
        pending = state['pending']
        if target < through or (pending and target < pending['end_row']):
            raise ValueError('Cible inférieure au préfixe validé ou au lot en attente')
        while through < target:
            end = state['pending']['end_row'] if state['pending'] else min(through + batch_rows, target)
            first = through + 1
            state['pending'] = {'start_row': first, 'end_row': end}
            # Record intent before any publication. On failure, replay this same range.
            save(cursor, state)
            print(f'→ Lot CSV {first}–{end} ; cible totale {target}', flush=True)
            result = pipeline.run_pipeline(csv, end, rate, max_offsets,
                                           directory / f'{first:09d}-{end:09d}', start_row=first)
            if result:
                print(f'Reprise conservée à {through} ; lot {first}–{end} à retenter.', file=sys.stderr)
                return result
            published = json.loads(snapshot.read_text())
            if published['counts']['fact_transaction'] != end:
                raise ValueError('Étoile publiée incompatible avec le lot validé')
            state.update(validated_through=end, pending=None, snapshot=published)
            save(cursor, state)
            through = end
        print(f'PASS: reprise CSV validée jusqu’à la ligne {through}. Curseur : {cursor}', flush=True)
        return 0
    except (Exception, KeyboardInterrupt) as error:
        print(f'ERREUR reprise CSV : {error or "interruption"}', file=sys.stderr, flush=True)
        return 1
