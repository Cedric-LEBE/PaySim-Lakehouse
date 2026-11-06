"""Silver quality rules, batch reference and incremental command entry point."""
import argparse
from pathlib import Path
from banking.bronze import session

DATASET = 'paysim-sha256-16910f90577b0d981bf8ff289714510bb89bc71bff7d3f220f024e287e4eea6b'
MONEY = ['amount', 'oldbalanceOrg', 'newbalanceOrig', 'oldbalanceDest', 'newbalanceDest']
FIELDS = ['step', 'type', *MONEY, 'nameOrig', 'nameDest', 'isFraud', 'isFlaggedFraud']


def exact_money(raw, value):
    """Accept finite unsigned decimals exactly representable at scale two."""
    from pyspark.sql import functions as F
    mantissa = F.regexp_extract(raw, r'^([0-9]+(?:\.[0-9]+)?)', 1)
    fraction = F.regexp_extract(mantissa, r'\.([0-9]+)$', 1)
    digits = F.regexp_replace(mantissa, r'\.', '')
    trailing_zeros = F.length(digits) - F.length(F.regexp_replace(digits, '0+$', ''))
    exponent_text = F.regexp_extract(raw, r'[eE]([+-]?[0-9]+)$', 1)
    exponent = F.when(exponent_text == '', F.lit(0)).otherwise(exponent_text.try_cast('long'))
    # The cast enforces Decimal(20,2) range; the scale check prevents rounding.
    scale = F.length(fraction) - trailing_zeros - exponent
    return (raw.rlike(r'^[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$')
            & exponent.between(-1000, 1000) & ((scale <= 2) | digits.rlike(r'^0+$'))
            & value.isNotNull() & (value >= 0))


def classify(bronze, dataset):
    from pyspark.sql import functions as F
    frame = bronze
    for name in ['event_id', 'dataset_id', 'source_row', 'schema_version']:
        frame = frame.withColumn(name, F.col('event.' + name))
    for name in FIELDS:
        value = F.col('event.transaction.' + name)
        if name in MONEY:
            value = F.expr(f'try_cast(event.transaction.{name} as decimal(20,2))')
        frame = frame.withColumn(name, value)
    rules = [
        ('excluded_dataset', F.col('dataset_id') == dataset),
        ('invalid_envelope', (F.col('parse_status') == 'parsed') & (F.col('schema_version') == 1)),
        ('invalid_source_row', F.col('source_row') >= 1),
        ('invalid_event_id', F.col('event_id') == F.sha2(F.concat_ws(':', 'dataset_id', F.col('source_row').cast('string')), 256)),
        ('invalid_step', F.col('step') >= 1),
        ('invalid_type', F.col('type').isin('PAYMENT', 'TRANSFER', 'CASH_OUT', 'CASH_IN', 'DEBIT')),
        ('invalid_origin', F.length(F.trim('nameOrig')) > 0),
        ('invalid_destination', F.length(F.trim('nameDest')) > 0),
        ('invalid_key', F.col('raw_key').cast('string') == F.col('nameOrig')),
        ('invalid_isFraud', F.col('isFraud').isin(0, 1)),
        ('invalid_isFlaggedFraud', F.col('isFlaggedFraud').isin(0, 1)),
    ]
    for name in MONEY:
        rules.append(('invalid_' + name,
                      exact_money(F.col('event.transaction.' + name), F.col(name))))
    frame = frame.withColumn('quality_reasons', F.filter(F.array(*[
        F.when(~F.coalesce(condition, F.lit(False)), F.lit(reason)) for reason, condition in rules
    ]), lambda reason: reason.isNotNull()))
    canonical = F.to_json(F.struct(*[F.col(name) for name in ['dataset_id', 'source_row', *FIELDS]]))
    frame = frame.withColumn('content_hash', F.sha2(canonical, 256))
    conflicts = (frame.filter(F.col('dataset_id') == dataset).groupBy('event_id')
                 .agg(F.countDistinct('content_hash').alias('_versions')))
    frame = frame.join(conflicts, 'event_id', 'left')
    return (frame.withColumn('quality_reasons', F.when(F.col('_versions') > 1,
                        F.array_union('quality_reasons', F.array(F.lit('conflicting_event_id'))))
                        .otherwise(F.col('quality_reasons'))).drop('_versions'))


def build(spark, source, output, dataset=DATASET):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F, Window
    source, output = Path(source).resolve(), Path(output).resolve()
    if source == output or source in output.parents or output in source.parents:
        raise ValueError('Bronze et Silver doivent être dans des dossiers distincts sans imbrication')
    if (output / '_incremental.json').exists():
        raise ValueError('Destination incrémentale : choisir une autre sortie pour le batch complet')
    if not (source / '_delta_log').exists():
        raise ValueError('Table Bronze Delta absente')
    version = DeltaTable.forPath(spark, str(source)).history(1).select('version').first()[0]
    bronze = spark.read.format('delta').option('versionAsOf', version).load(str(source))
    checked = classify(bronze, dataset).persist()
    try:
        valid = checked.filter(F.size('quality_reasons') == 0)
        window = Window.partitionBy('event_id').orderBy(F.col('kafka_timestamp').asc_nulls_last(),
                                                      'topic', 'partition', 'offset')
        clean = (valid.withColumn('_rank', F.row_number().over(window)).filter('_rank = 1')
                 .select('event_id', 'dataset_id', 'source_row', *FIELDS, 'content_hash',
                         'topic', 'partition', 'offset', 'kafka_timestamp', 'ingested_at'))
        rejected = checked.filter(F.size('quality_reasons') > 0)
        input_count, valid_count = checked.count(), valid.count()
        clean_count, rejected_count = clean.count(), rejected.count()
        if input_count != valid_count + rejected_count:
            raise RuntimeError('Comptage qualité incohérent')
        for name, frame in [('transactions', clean), ('quarantine', rejected)]:
            (frame.withColumn('bronze_version', F.lit(version)).write.format('delta')
             .mode('overwrite').option('overwriteSchema', 'true').save(str(output / name)))
        result = {'bronze_version': version, 'bronze_rows': input_count, 'silver_rows': clean_count,
                  'quarantine_rows': rejected_count, 'duplicates_removed': valid_count - clean_count}
        print('PASS: Silver : ' + str(result), flush=True)
        return result
    finally:
        checked.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', default='data/bronze/transactions')
    parser.add_argument('--output', default='data/silver')
    parser.add_argument('--dataset-id', default=DATASET)
    parser.add_argument('--mode', choices=['incremental', 'batch'], default='incremental')
    args = parser.parse_args()
    if not args.dataset_id.strip():
        parser.error('dataset-id non vide requis')
    spark = session()
    spark.sparkContext.setLogLevel('WARN')
    try:
        if args.mode == 'incremental':
            from banking.silver_incremental import build as incremental
            incremental(spark, args.source, args.output, args.dataset_id)
        else:
            build(spark, args.source, args.output, args.dataset_id)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
