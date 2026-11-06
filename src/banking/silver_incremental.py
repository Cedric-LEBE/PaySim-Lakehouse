"""Local append-only Bronze CDF ingestion with replay-safe Silver MERGEs."""
import fcntl
import json
from pathlib import Path

from banking.silver import DATASET, FIELDS, classify


def ensure_published(source):
    marker = Path(source).resolve().parent / '_incremental.json'
    if marker.exists() and 'pending_version' in json.loads(marker.read_text()):
        raise ValueError('Silver partiellement publié : reprendre Silver avant Gold ou étoile')


def publish(path, state):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(state, indent=2) + '\n')
    temporary.replace(path)


def merge(spark, path, source, condition, bootstrap=False, delete=False):
    from delta.tables import DeltaTable
    if not (path / '_delta_log').exists():
        frame = source.filter('NOT _delete').drop('_delete') if delete else source
        frame.write.format('delta').save(str(path))
        return
    operation = DeltaTable.forPath(spark, str(path)).alias('t').merge(source.alias('s'), condition)
    columns = {c: 's.`' + c + '`' for c in source.columns if c != '_delete'}
    if delete:
        operation = operation.whenMatchedDelete('s._delete')
        operation = operation.whenMatchedUpdate(condition='NOT s._delete', set=columns)
        operation = operation.whenNotMatchedInsert(condition='NOT s._delete', values=columns)
    else:
        operation = operation.whenMatchedUpdateAll().whenNotMatchedInsertAll()
    if bootstrap:
        operation = operation.whenNotMatchedBySourceDelete()
    operation.execute()


def build(spark, source, output, dataset=DATASET):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F, Window
    from pyspark import StorageLevel
    spark.conf.set('spark.sql.shuffle.partitions', '8')
    spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')
    spark.conf.set('spark.sql.adaptive.autoBroadcastJoinThreshold', '-1')
    source, output = Path(source).resolve(), Path(output).resolve()
    if source == output or source in output.parents or output in source.parents:
        raise ValueError('Bronze et Silver doivent être distincts sans imbrication')
    if not (source / '_delta_log').exists():
        raise ValueError('Table Bronze absente')
    output.mkdir(parents=True, exist_ok=True)
    with (output / '.incremental.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError('Silver incrémental déjà en cours') from None
        table = DeltaTable.forPath(spark, str(source))
        identity = {'format_version': 1, 'source': str(source), 'source_id': table.detail().first()['id'],
                    'output': str(output), 'dataset_id': dataset}
        marker = output / '_incremental.json'
        if marker.exists():
            state = json.loads(marker.read_text())
            if any(state.get(k) != v for k, v in identity.items()):
                raise ValueError('État Silver associé à une autre source ou un autre dataset')
        else:
            # Enable CDF before choosing the initial snapshot, without changing Bronze rows.
            spark.sql(f"ALTER TABLE delta.`{str(source).replace('`', '``')}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
            state = {**identity, 'bronze_version': -1}
            publish(marker, state)
        if state['bronze_version'] >= 0:
            for name in ('_events', 'transactions', 'quarantine'):
                if not (output / name / '_delta_log').exists():
                    raise ValueError('État Silver incomplet : table manquante ' + name)
        latest = table.history(1).first()['version']
        if latest < state['bronze_version']:
            raise ValueError('Version Bronze antérieure à l’état Silver')
        if latest == state['bronze_version'] and 'pending_version' not in state:
            result = {'bronze_version': latest, 'new_bronze_rows': 0, 'status': 'no_change'}
            print('PASS: Silver incrémental : ' + str(result), flush=True)
            return result
        bootstrap = state['bronze_version'] == -1
        if 'pending_version' not in state:
            state['pending_version'] = latest
            publish(marker, state)
        end = state['pending_version']
        if bootstrap:
            incoming = (spark.read.format('delta').option('versionAsOf', end).load(str(source))
                        .withColumn('bronze_version', F.lit(end).cast('long')))
        else:
            incoming = (spark.read.format('delta').option('readChangeFeed', 'true')
                        .option('startingVersion', state['bronze_version'] + 1)
                        .option('endingVersion', end).load(str(source)))
            if incoming.filter(F.col('_change_type') != 'insert').limit(1).count():
                raise ValueError('Bronze doit rester append-only : modification/suppression refusée')
            incoming = (incoming.withColumnRenamed('_commit_version', 'bronze_version')
                        .drop('_change_type', '_commit_timestamp'))
        incoming = incoming.persist(StorageLevel.DISK_ONLY)
        checked = None
        try:
            count = incoming.count()
            keys = ['topic', 'partition', 'offset']
            if incoming.filter(F.col('topic').isNull() | F.col('partition').isNull() | F.col('offset').isNull()).limit(1).count():
                raise ValueError('Coordonnées Kafka absentes')
            if incoming.groupBy(*keys).count().filter('count > 1').limit(1).count():
                raise ValueError('Coordonnées Kafka dupliquées dans Bronze')
            ledger = output / '_events'
            merge(spark, ledger, incoming, 't.topic = s.topic AND t.partition = s.partition AND t.offset = s.offset')
            events = spark.read.format('delta').load(str(ledger))
            if not bootstrap:
                touched = incoming.select(F.col('event.event_id').alias('id')).distinct()
                events = events.join(touched, F.col('event.event_id').eqNullSafe(F.col('id')), 'left_semi')
            checked = classify(events, dataset).persist(StorageLevel.DISK_ONLY)
            valid = checked.filter(F.size('quality_reasons') == 0)
            order = Window.partitionBy('event_id').orderBy(F.col('kafka_timestamp').asc_nulls_last(), 'topic', 'partition', 'offset')
            columns = ['event_id', 'dataset_id', 'source_row', *FIELDS, 'content_hash',
                       'topic', 'partition', 'offset', 'kafka_timestamp', 'ingested_at', 'bronze_version']
            clean = (valid.withColumn('_rank', F.row_number().over(order)).filter('_rank = 1').select(*columns))
            # A conflict across commits invalidates the formerly accepted event too.
            removed = (checked.filter(F.array_contains('quality_reasons', 'conflicting_event_id'))
                       .filter(F.col('dataset_id') == dataset).select(*columns).dropDuplicates(['event_id']))
            decisions = clean.withColumn('_delete', F.lit(False)).unionByName(removed.withColumn('_delete', F.lit(True)))
            if not (output / 'transactions/_delta_log').exists():
                clean.limit(0).write.format('delta').save(str(output / 'transactions'))
            merge(spark, output / 'transactions', decisions, 't.event_id = s.event_id', bootstrap, delete=True)
            rejected = checked.filter(F.size('quality_reasons') > 0)
            merge(spark, output / 'quarantine', rejected,
                  't.topic = s.topic AND t.partition = s.partition AND t.offset = s.offset', bootstrap)
            state['bronze_version'] = end
            state.pop('pending_version')
            publish(marker, state)  # Never advance before all three Delta writes succeeded.
            result = {'bronze_version': end, 'new_bronze_rows': count,
                      'silver_rows': spark.read.format('delta').load(str(output / 'transactions')).count(),
                      'quarantine_rows': spark.read.format('delta').load(str(output / 'quarantine')).count()}
            print('PASS: Silver incrémental : ' + str(result), flush=True)
            return result
        finally:
            incoming.unpersist()
            if checked is not None:
                checked.unpersist()
