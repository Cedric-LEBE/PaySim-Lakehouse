"""Publish a transaction-grain Delta star with stable keys and pinned SQL views."""
import argparse
import json
from pathlib import Path
from banking.bronze import session
from banking.silver import MONEY

TABLE_KEYS = {'dim_transaction_type': 'transaction_type_key',
              'dim_simulation_time': 'simulation_time_key', 'dim_account': 'account_key',
              'fact_transaction': 'transaction_key'}


def key(domain, *columns):
    from pyspark.sql import functions as F
    return F.sha2(F.to_json(F.array(F.lit(domain), *[F.col(c).cast('string') for c in columns])), 256)


def frames(source):
    from pyspark.sql import functions as F
    types = (source.select(F.col('type').alias('transaction_type')).distinct()
             .withColumn('transaction_type_key', key('transaction_type', 'transaction_type')))
    time = (source.select('dataset_id', F.col('step').alias('simulation_step')).distinct()
            .withColumn('simulation_time_key', key('simulation_time', 'dataset_id', 'simulation_step'))
            .withColumn('simulation_day_index', F.floor((F.col('simulation_step') - 1) / 24).cast('long') + 1)
            .withColumn('hour_index_in_simulated_day', F.pmod(F.col('simulation_step') - 1, F.lit(24))))
    accounts = (source.select('dataset_id', F.col('nameOrig').alias('account_id'))
                .unionByName(source.select('dataset_id', F.col('nameDest').alias('account_id'))).distinct()
                .withColumn('account_key', key('account', 'dataset_id', 'account_id'))
                .withColumn('account_kind', F.when(F.col('account_id').startswith('M'), 'merchant')
                            .when(F.col('account_id').startswith('C'), 'customer').otherwise('unknown')))
    fact = (source.withColumn('transaction_type', F.col('type'))
            .withColumn('simulation_step', F.col('step'))
            .withColumn('origin_account_id', F.col('nameOrig'))
            .withColumn('destination_account_id', F.col('nameDest'))
            .select(F.col('event_id').alias('transaction_key'), 'dataset_id', 'source_row',
                    key('transaction_type', 'transaction_type').alias('transaction_type_key'),
                    key('simulation_time', 'dataset_id', 'simulation_step').alias('simulation_time_key'),
                    key('account', 'dataset_id', 'origin_account_id').alias('origin_account_key'),
                    key('account', 'dataset_id', 'destination_account_id').alias('destination_account_key'),
                    F.lit(1).cast('long').alias('transaction_count'), F.col('amount'),
                    F.col('oldbalanceOrg').alias('origin_balance_before'),
                    F.col('newbalanceOrig').alias('origin_balance_after'),
                    F.col('oldbalanceDest').alias('destination_balance_before'),
                    F.col('newbalanceDest').alias('destination_balance_after'),
                    F.col('isFraud').alias('is_fraud'), F.col('isFlaggedFraud').alias('is_flagged_fraud'),
                    'topic', 'partition', 'offset', 'kafka_timestamp', 'ingested_at', 'bronze_version'))
    return {'dim_transaction_type': types, 'dim_simulation_time': time,
            'dim_account': accounts, 'fact_transaction': fact}


def build(spark, source, output):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType
    source, output = Path(source).resolve(), Path(output).resolve()
    if source == output or source in output.parents or output in source.parents:
        raise ValueError('Silver et étoile doivent être distinctes sans imbrication')
    if not (source / '_delta_log').exists():
        raise ValueError('Table Silver absente')
    version = DeltaTable.forPath(spark, str(source)).history(1).select('version').first()[0]
    silver = spark.read.format('delta').option('versionAsOf', version).load(str(source)).persist()
    try:
        if any(silver.schema[name].dataType != DecimalType(20, 2) for name in MONEY):
            raise ValueError('Montants Silver attendus en Decimal(20,2)')
        valid = ((F.length(F.trim('dataset_id')) > 0) & (F.col('source_row') >= 1)
                 & F.col('bronze_version').isNotNull() & (F.col('step') >= 1)
                 & F.col('type').isin('PAYMENT', 'TRANSFER', 'CASH_OUT', 'CASH_IN', 'DEBIT')
                 & (F.length(F.trim('nameOrig')) > 0) & (F.length(F.trim('nameDest')) > 0)
                 & F.col('isFraud').isin(0, 1) & F.col('isFlaggedFraud').isin(0, 1))
        for name in MONEY:
            valid = valid & (F.col(name) >= 0)
        check = silver.agg(F.count('*').alias('rows'), F.countDistinct('event_id').alias('ids'),
                           F.countDistinct('dataset_id').alias('datasets'),
                           F.countDistinct('bronze_version').alias('bronze_versions'),
                           F.count(F.when(~F.coalesce(valid, F.lit(False)), True)).alias('invalid')).first()
        if check.invalid or check.rows != check.ids or check.datasets > 1 or check.bronze_versions > 1:
            raise ValueError('Silver invalide : doublons, valeurs invalides ou snapshots mélangés')
        tables = frames(silver)
        counts = {}
        for name, table in tables.items():
            primary = TABLE_KEYS[name]
            stats = table.agg(F.count('*').alias('rows'), F.countDistinct(primary).alias('ids')).first()
            if stats.rows != stats.ids:
                raise ValueError('Clé primaire absente ou non unique : ' + name)
            counts[name] = stats.rows
        fact = tables['fact_transaction']
        for fk, dimension, pk in [
            ('transaction_type_key', 'dim_transaction_type', 'transaction_type_key'),
            ('simulation_time_key', 'dim_simulation_time', 'simulation_time_key'),
            ('origin_account_key', 'dim_account', 'account_key'),
            ('destination_account_key', 'dim_account', 'account_key')]:
            if fact.join(tables[dimension], fact[fk] == tables[dimension][pk], 'left_anti').limit(1).count():
                raise ValueError('Référence dimension absente : ' + fk)
        if counts['fact_transaction'] != check.rows:
            raise RuntimeError('Le grain transaction a changé')
        versions = {}
        for name, table in tables.items():
            path = str(output / name)
            (table.withColumn('silver_version', F.lit(version)).write.format('delta')
             .mode('overwrite').option('overwriteSchema', 'true').save(path))
            versions[name] = DeltaTable.forPath(spark, path).history(1).select('version').first()[0]
        manifest = {'format_version': 1, 'silver_path': str(source), 'silver_version': version,
                    'table_versions': versions, 'counts': counts}
        temporary = output / '_snapshot.json.tmp'
        temporary.write_text(json.dumps(manifest, indent=2) + '\n')
        temporary.replace(output / '_snapshot.json')
        print(f'PASS: Étoile : {counts}; références et clés uniques validées.', flush=True)
        return manifest
    finally:
        silver.unpersist()


def register_views(spark, root):
    root = Path(root).resolve()
    manifest = json.loads((root / '_snapshot.json').read_text())
    if manifest.get('format_version') != 1 or set(manifest['table_versions']) != set(TABLE_KEYS):
        raise ValueError('Manifeste étoile invalide')
    for name, version in manifest['table_versions'].items():
        (spark.read.format('delta').option('versionAsOf', version).load(str(root / name))
         .createOrReplaceTempView(name))
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', default='data/silver/transactions')
    parser.add_argument('--output', default='data/gold/star')
    args = parser.parse_args()
    spark = session()
    spark.sparkContext.setLogLevel('WARN')
    try:
        build(spark, args.source, args.output)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
