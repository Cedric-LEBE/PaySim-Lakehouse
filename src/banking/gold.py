"""Descriptive fraud metrics from one fixed Silver Delta snapshot."""
import argparse
from pathlib import Path
from banking.bronze import session


def metrics(frame, keys=()):
    from pyspark.sql import functions as F
    zero = F.lit('0.00').cast('decimal(30,2)')
    grouped = frame.groupBy(*keys).agg(
        F.count('*').alias('total_transactions'),
        F.coalesce(F.sum('amount'), zero).alias('total_amount'),
        F.avg('amount').cast('decimal(24,6)').alias('avg_amount'),
        F.count(F.when(F.col('isFraud') == 1, True)).alias('fraud_transactions'),
        F.coalesce(F.sum(F.when(F.col('isFraud') == 1, F.col('amount'))), zero).alias('fraud_amount'),
        F.count(F.when(F.col('isFlaggedFraud') == 1, True)).alias('flagged_transactions'))
    return grouped.withColumn('fraud_rate', F.when(F.col('total_transactions') > 0,
                  F.col('fraud_transactions') / F.col('total_transactions')).cast('decimal(18,8)'))


def validate(frame):
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType
    required = {'event_id', 'dataset_id', 'step', 'type', 'amount', 'isFraud', 'isFlaggedFraud', 'bronze_version'}
    if not required.issubset(frame.columns):
        raise ValueError('Colonnes Silver manquantes')
    if frame.schema['amount'].dataType != DecimalType(20, 2):
        raise ValueError('amount doit être Decimal(20,2)')
    valid = (F.col('event_id').isNotNull() & (F.length(F.trim('dataset_id')) > 0)
             & F.col('bronze_version').isNotNull() & (F.col('step') >= 1)
             & F.col('type').isin('PAYMENT', 'TRANSFER', 'CASH_OUT', 'CASH_IN', 'DEBIT')
             & (F.col('amount') >= 0) & F.col('isFraud').isin(0, 1)
             & F.col('isFlaggedFraud').isin(0, 1))
    check = frame.agg(F.count('*').alias('rows'), F.countDistinct('event_id').alias('ids'),
                      F.countDistinct('dataset_id').alias('datasets'),
                      F.count(F.when(~F.coalesce(valid, F.lit(False)), True)).alias('invalid'),
                      F.min('dataset_id').alias('dataset_id'),
                      F.max('bronze_version').alias('bronze_version')).first()
    if check.invalid or check.rows != check.ids or check.datasets > 1:
        raise ValueError('Silver invalide : doublons, valeurs invalides ou sources mélangées')
    return check


def build(spark, source, output):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType
    source, output = Path(source).resolve(), Path(output).resolve()
    if source == output or source in output.parents or output in source.parents:
        raise ValueError('Silver et Gold doivent être dans des dossiers distincts sans imbrication')
    if not (source / '_delta_log').exists():
        raise ValueError('Table Silver Delta absente')
    from banking.silver_incremental import ensure_published
    ensure_published(source)
    if (output / '_gold_incremental.json').exists():
        raise ValueError('Destination Gold incrémentale : choisir une sortie batch distincte')
    version = DeltaTable.forPath(spark, str(source)).history(1).select('version').first()[0]
    frame = spark.read.format('delta').option('versionAsOf', version).load(str(source)).persist()
    try:
        check = validate(frame)
        tables = {'summary': metrics(frame), 'by_type': metrics(frame, ('type',)),
                  'by_step': metrics(frame, ('step',))}
        summary = tables['summary'].first().asDict()
        for name in ['by_type', 'by_step']:
            totals = tables[name].agg(*[F.sum(field).alias(field) for field in
                      ['total_transactions', 'total_amount', 'fraud_transactions', 'fraud_amount']]).first()
            if any((totals[field] or 0) != summary[field] for field in totals.asDict()):
                raise RuntimeError('Agrégats incohérents : ' + name)
        for name, table in tables.items():
            enriched = (table.withColumn('silver_version', F.lit(version))
                        .withColumn('bronze_version', F.lit(check.bronze_version).cast('long'))
                        .withColumn('dataset_id', F.lit(check.dataset_id).cast('string')))
            (enriched.write.format('delta').mode('overwrite').option('overwriteSchema', 'true')
             .save(str(output / name)))
        rate = 'non défini' if summary['fraud_rate'] is None else f"{summary['fraud_rate'] * 100:.2f} %"
        print(f"PASS: Gold : {summary['total_transactions']} transactions, "
              f"{summary['fraud_transactions']} transactions frauduleuses ({rate}), "
              f"montant total={summary['total_amount']}, montant fraude={summary['fraud_amount']}.", flush=True)
        return {'silver_version': version, 'bronze_version': check.bronze_version, **summary}
    finally:
        frame.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', default='data/silver/transactions')
    parser.add_argument('--output', default='data/gold')
    parser.add_argument('--mode', choices=['incremental', 'batch'], default='incremental')
    args = parser.parse_args()
    spark = session()
    spark.sparkContext.setLogLevel('WARN')
    try:
        if args.mode == 'incremental':
            from banking.gold_incremental import build as incremental
            incremental(spark, args.source, args.output)
        else:
            build(spark, args.source, args.output)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
