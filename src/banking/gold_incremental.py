"""Replay-safe signed Gold aggregates from Silver CDF, with pinned base versions."""
import fcntl
import json
from pathlib import Path
from banking.gold import metrics, validate
from banking.silver_incremental import ensure_published, publish, merge

TABLES = {'summary': None, 'by_type': 'type', 'by_step': 'step'}
COUNTERS = ['total_transactions', 'total_amount', 'fraud_transactions', 'fraud_amount', 'flagged_transactions']


def build(spark, source, output):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    from pyspark import StorageLevel
    source, output = Path(source).resolve(), Path(output).resolve()
    if source == output or source in output.parents or output in source.parents:
        raise ValueError('Silver et Gold doivent être distincts sans imbrication')
    ensure_published(source)
    table = DeltaTable.forPath(spark, str(source))
    output.mkdir(parents=True, exist_ok=True)
    with (output / '.gold-incremental.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError('Gold incrémental déjà en cours') from None
        marker = output / '_gold_incremental.json'
        identity = {'format_version': 1, 'source': str(source),
                    'source_id': table.detail().first()['id'], 'output': str(output)}
        if marker.exists():
            state = json.loads(marker.read_text())
            if any(state.get(k) != v for k, v in identity.items()):
                raise ValueError('État Gold associé à une autre source')
        else:
            spark.sql(f"ALTER TABLE delta.`{str(source).replace('`', '``')}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
            state = {**identity, 'silver_version': -1}
            publish(marker, state)
        latest = table.history(1).first()['version']
        if latest < state['silver_version']:
            raise ValueError('Silver antérieur au curseur Gold')
        if state['silver_version'] >= 0:
            for name in TABLES:
                if not (output / name / '_delta_log').exists():
                    raise ValueError('Table Gold manquante : ' + name)
        if latest == state['silver_version'] and 'pending' not in state:
            result = {'silver_version': latest, 'changed_rows': 0, 'status': 'no_change'}
            print('PASS: Gold incrémental : ' + str(result), flush=True)
            return result
        bootstrap = state['silver_version'] == -1
        if 'pending' not in state:
            bases = {} if bootstrap else {name: DeltaTable.forPath(spark, str(output / name)).history(1).first()['version'] for name in TABLES}
            state['pending'] = {'end': latest, 'base_versions': bases}
            publish(marker, state)
        end = state['pending']['end']
        snapshot = spark.read.format('delta').option('versionAsOf', end).load(str(source)).persist(StorageLevel.DISK_ONLY)
        changes = None
        updates = {}
        try:
            # Validation still scans the pinned snapshot; business aggregates use CDF deltas.
            check = validate(snapshot)
            if bootstrap:
                changed_count = check.rows
            else:
                changes = (spark.read.format('delta').option('readChangeFeed', 'true')
                           .option('startingVersion', state['silver_version'] + 1)
                           .option('endingVersion', end).load(str(source)).persist(StorageLevel.DISK_ONLY))
                if changes.filter(~F.col('_change_type').isin('insert', 'delete', 'update_preimage', 'update_postimage')).limit(1).count():
                    raise ValueError('Type CDF Silver inconnu')
                changed_count = changes.count()
            for name, key in TABLES.items():
                if bootstrap:
                    frame = metrics(snapshot, (key,) if key else ())
                else:
                    signed = changes.withColumn('_sign', F.when(F.col('_change_type').isin('delete', 'update_preimage'), -1).otherwise(1))
                    delta = signed.groupBy(*([key] if key else [])).agg(
                        F.coalesce(F.sum('_sign'), F.lit(0)).alias('d_total_transactions'),
                        F.coalesce(F.sum(F.col('_sign') * F.col('amount')), F.lit(0)).alias('d_total_amount'),
                        F.coalesce(F.sum(F.col('_sign') * F.col('isFraud')), F.lit(0)).alias('d_fraud_transactions'),
                        F.coalesce(F.sum(F.when(F.col('isFraud') == 1, F.col('_sign') * F.col('amount'))), F.lit(0)).alias('d_fraud_amount'),
                        F.coalesce(F.sum(F.col('_sign') * F.col('isFlaggedFraud')), F.lit(0)).alias('d_flagged_transactions'))
                    # Avoid Decimal(38,2) addition reducing scale to one decimal in Spark.
                    for amount in ('d_total_amount', 'd_fraud_amount'):
                        delta = delta.withColumn(amount, F.col(amount).cast('decimal(30,2)'))
                    base = spark.read.format('delta').option('versionAsOf', state['pending']['base_versions'][name]).load(str(output / name))
                    joined = delta.alias('d').join(base.alias('b'), key if key else F.lit(True), 'left')
                    frame = joined.select(*([F.col(key)] if key else []), *[
                        (F.coalesce(F.col('b.' + c), F.lit(0)) + F.col('d.d_' + c))
                        .cast('decimal(30,2)' if c.endswith('amount') else 'long').alias(c) for c in COUNTERS])
                    if frame.filter((F.col('total_transactions') < 0) | (F.col('fraud_transactions') < 0) | (F.col('fraud_transactions') > F.col('total_transactions'))).limit(1).count():
                        raise ValueError('Compteurs Gold incohérents')
                    frame = (frame.withColumn('avg_amount', F.when(F.col('total_transactions') > 0, F.col('total_amount') / F.col('total_transactions')).cast('decimal(24,6)'))
                             .withColumn('fraud_rate', F.when(F.col('total_transactions') > 0, F.col('fraud_transactions') / F.col('total_transactions')).cast('decimal(18,8)')))
                updates[name] = (frame.withColumn('silver_version', F.lit(end))
                                 .withColumn('bronze_version', F.lit(check.bronze_version).cast('long'))
                                 .withColumn('dataset_id', F.lit(check.dataset_id).cast('string'))
                                 .localCheckpoint(eager=True, storageLevel=StorageLevel.DISK_ONLY))
            # Assign absolute values from pinned bases, never add onto a possibly retried write.
            for name, frame in updates.items():
                key = TABLES[name]
                decisions = frame.withColumn('_delete', (F.col('total_transactions') == 0) if key else F.lit(False))
                merge(spark, output / name, decisions, f't.{key} = s.{key}' if key else 'true', bootstrap, delete=True)
            state['silver_version'] = end
            state.pop('pending')
            state['table_versions'] = {name: DeltaTable.forPath(spark, str(output / name)).history(1).first()['version'] for name in TABLES}
            publish(marker, state)
            summary = spark.read.format('delta').load(str(output / 'summary')).first().asDict()
            result = {'silver_version': end, 'changed_rows': changed_count, **summary}
            print('PASS: Gold incrémental : ' + str(result), flush=True)
            return result
        finally:
            for frame in updates.values():
                frame.unpersist()
            snapshot.unpersist()
            if changes is not None:
                changes.unpersist()


def read_tables(spark, output):
    """Read the last published Gold versions; refuse an incomplete publication."""
    output = Path(output).resolve()
    marker = output / '_gold_incremental.json'
    state = json.loads(marker.read_text()) if marker.exists() else None
    if state and ('pending' in state or state['silver_version'] < 0):
        raise ValueError('Gold partiellement publié : reprendre Gold avant lecture')
    return {name: (spark.read.format('delta').option('versionAsOf', state['table_versions'][name]).load(str(output / name))
                   if state else spark.read.format('delta').load(str(output / name))) for name in TABLES}
