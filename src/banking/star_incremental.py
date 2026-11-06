"""Incremental fact MERGE and insert-only dimensions, published through one manifest."""
import fcntl
import json
from pathlib import Path
from banking.star import TABLE_KEYS, frames, validate
from banking.silver_incremental import ensure_published, publish, merge


def build(spark, source, output):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    from pyspark import StorageLevel
    source, output = Path(source).resolve(), Path(output).resolve()
    if source == output or source in output.parents or output in source.parents:
        raise ValueError('Silver et étoile doivent être distincts sans imbrication')
    ensure_published(source)
    table = DeltaTable.forPath(spark, str(source))
    output.mkdir(parents=True, exist_ok=True)
    with (output / '.star-incremental.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError('Étoile incrémentale déjà en cours') from None
        marker = output / '_star_incremental.json'
        identity = {'format_version': 1, 'source': str(source),
                    'source_id': table.detail().first()['id'], 'output': str(output)}
        if marker.exists():
            state = json.loads(marker.read_text())
            if any(state.get(k) != v for k, v in identity.items()):
                raise ValueError('État étoile associé à une autre source')
        else:
            # Avoid a metadata-only source commit when Gold already enabled CDF.
            if table.detail().first()['properties'].get('delta.enableChangeDataFeed') != 'true':
                spark.sql(f"ALTER TABLE delta.`{str(source).replace('`', '``')}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
            state = {**identity, 'silver_version': -1}
            publish(marker, state)
        latest = table.history(1).first()['version']
        if latest < state['silver_version']:
            raise ValueError('Silver antérieur au curseur étoile')
        if state['silver_version'] >= 0:
            for name in TABLE_KEYS:
                if not (output / name / '_delta_log').exists():
                    raise ValueError('Table étoile manquante : ' + name)
            if not (output / '_snapshot.json').exists():
                raise ValueError('Manifeste étoile manquant')
        if latest == state['silver_version'] and 'pending_version' not in state:
            result = {'silver_version': latest, 'changed_rows': 0, 'status': 'no_change'}
            print('PASS: Étoile incrémentale : ' + str(result), flush=True)
            return result
        bootstrap = state['silver_version'] == -1
        if 'pending_version' not in state:
            state['pending_version'] = latest
            publish(marker, state)
        end = state['pending_version']
        snapshot = spark.read.format('delta').option('versionAsOf', end).load(str(source)).persist(StorageLevel.DISK_ONLY)
        changes = None
        decisions = None
        try:
            check = validate(snapshot)
            if bootstrap:
                changes = snapshot
                current = snapshot
                count = check.rows
            else:
                changes = (spark.read.format('delta').option('readChangeFeed', 'true')
                           .option('startingVersion', state['silver_version'] + 1)
                           .option('endingVersion', end).load(str(source)).persist(StorageLevel.DISK_ONLY))
                if changes.filter(~F.col('_change_type').isin('insert', 'delete', 'update_preimage', 'update_postimage')).limit(1).count():
                    raise ValueError('Type CDF Silver inconnu')
                count = validate(changes, unique=False).rows
                touched = changes.select('event_id').distinct()
                current = snapshot.join(touched, 'event_id', 'left_semi')
            dimensions = frames(changes)
            # Dimension rows are never removed, even when all their facts disappear.
            for name, primary in TABLE_KEYS.items():
                if name == 'fact_transaction':
                    continue
                incoming = dimensions[name].withColumn('silver_version', F.lit(end))
                path = output / name
                if not (path / '_delta_log').exists():
                    incoming.write.format('delta').save(str(path))
                else:
                    (DeltaTable.forPath(spark, str(path)).alias('t').merge(incoming.alias('s'), f't.{primary} = s.{primary}')
                     .whenNotMatchedInsertAll().execute())
            facts = frames(current)['fact_transaction'].withColumn('silver_version', F.lit(end))
            decisions = facts.withColumn('_delete', F.lit(False))
            if not bootstrap:
                deleted = (dimensions['fact_transaction'].join(facts.select('transaction_key'), 'transaction_key', 'left_anti')
                           .dropDuplicates(['transaction_key']).withColumn('silver_version', F.lit(end))
                           .withColumn('_delete', F.lit(True)))
                decisions = decisions.unionByName(deleted)
            decisions = decisions.localCheckpoint(eager=True, storageLevel=StorageLevel.DISK_ONLY)
            merge(spark, output / 'fact_transaction', decisions, 't.transaction_key = s.transaction_key', bootstrap, delete=True)
            tables = {name: spark.read.format('delta').load(str(output / name)) for name in TABLE_KEYS}
            counts = {}
            for name, frame in tables.items():
                primary = TABLE_KEYS[name]
                stats = frame.agg(F.count('*').alias('rows'), F.countDistinct(primary).alias('ids')).first()
                if stats.rows != stats.ids:
                    raise ValueError('Clé étoile absente ou dupliquée : ' + name)
                counts[name] = stats.rows
            fact = tables['fact_transaction']
            for fk, dimension, pk in [
                ('transaction_type_key', 'dim_transaction_type', 'transaction_type_key'),
                ('simulation_time_key', 'dim_simulation_time', 'simulation_time_key'),
                ('origin_account_key', 'dim_account', 'account_key'),
                ('destination_account_key', 'dim_account', 'account_key')]:
                if fact.join(tables[dimension], fact[fk] == tables[dimension][pk], 'left_anti').limit(1).count():
                    raise ValueError('Référence dimension absente : ' + fk)
            if counts['fact_transaction'] != check.rows or fact.select('transaction_key').subtract(snapshot.select('event_id')).limit(1).count():
                raise ValueError('Faits différents du périmètre Silver')
            versions = {name: DeltaTable.forPath(spark, str(output / name)).history(1).first()['version'] for name in TABLE_KEYS}
            manifest = {'format_version': 1, 'silver_path': str(source), 'silver_version': end,
                        'table_versions': versions, 'counts': counts}
            publish(output / '_snapshot.json', manifest)  # Readers switch all four versions together.
            state['silver_version'] = end
            state.pop('pending_version')
            publish(marker, state)
            print(f'PASS: Étoile incrémentale : {counts}; changed_rows={count}, silver_version={end}.', flush=True)
            return {'changed_rows': count, **manifest}
        finally:
            if decisions is not None:
                decisions.unpersist()
            if changes is not None and not bootstrap:
                changes.unpersist()
            snapshot.unpersist()
