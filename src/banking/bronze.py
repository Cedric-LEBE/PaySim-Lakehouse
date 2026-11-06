"""Kafka to append-only Bronze Delta, retaining every raw payload."""
import argparse
import json
from pathlib import Path


def session():
    from pyspark.sql import SparkSession
    return (SparkSession.builder.appName('banking-bronze').master('local[2]')
            .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
            .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
            .config('spark.sql.shuffle.partitions', '2')
            .config('spark.default.parallelism', '2')
            .config('spark.databricks.delta.snapshotPartitions', '2')
            .config('spark.sql.session.timeZone', 'UTC')
            .config('spark.ui.enabled', 'false').getOrCreate())


def bronze_frame(raw):
    from pyspark.sql import functions as F, types as T
    transaction = T.StructType([
        T.StructField(name, kind) for name, kind in [
            ('step', T.LongType()), ('type', T.StringType()), ('amount', T.StringType()),
            ('nameOrig', T.StringType()), ('oldbalanceOrg', T.StringType()),
            ('newbalanceOrig', T.StringType()), ('nameDest', T.StringType()),
            ('oldbalanceDest', T.StringType()), ('newbalanceDest', T.StringType()),
            ('isFraud', T.IntegerType()), ('isFlaggedFraud', T.IntegerType())]])
    schema = T.StructType([T.StructField('schema_version', T.IntegerType()),
                           T.StructField('event_id', T.StringType()),
                           T.StructField('dataset_id', T.StringType()),
                           T.StructField('source_row', T.LongType()),
                           T.StructField('transaction', transaction),
                           T.StructField('_corrupt_record', T.StringType())])
    frame = raw.select('topic', 'partition', 'offset',
                       F.col('timestamp').alias('kafka_timestamp'), 'timestampType',
                       F.col('key').alias('raw_key'), F.col('value').alias('raw_value'))
    frame = frame.withColumn('payload', F.col('raw_value').cast('string'))
    frame = frame.withColumn('event', F.from_json('payload', schema, {'mode': 'PERMISSIVE'}))
    problem = (F.col('event').isNull() | F.col('event._corrupt_record').isNotNull()
               | F.col('event.schema_version').isNull() | (F.col('event.schema_version') != 1)
               | F.col('event.event_id').isNull() | F.col('event.dataset_id').isNull()
               | F.col('event.source_row').isNull() | F.col('event.transaction').isNull())
    frame = frame.withColumn('parse_status', F.when(problem, 'invalid_envelope').otherwise('parsed'))
    # No filtering: malformed, unsupported and replayed events remain in Bronze.
    return (frame.withColumn('amount', F.expr('try_cast(event.transaction.amount as decimal(20,2))'))
            .withColumn('ingested_at', F.current_timestamp())
            .withColumn('ingestion_date', F.to_date('ingested_at')))


def ingest(spark, bootstrap, topic, output, checkpoint, max_offsets=100):
    output, checkpoint = Path(output).resolve(), Path(checkpoint).resolve()
    if output == checkpoint or output in checkpoint.parents or checkpoint in output.parents:
        raise ValueError('Table et checkpoint doivent être dans des dossiers distincts sans imbrication')
    checkpoint.mkdir(parents=True, exist_ok=True)
    identity = {'bootstrap': bootstrap, 'topic': topic, 'output': str(output), 'schema_version': 1}
    marker = checkpoint / 'banking-source.json'
    if marker.exists() and json.loads(marker.read_text()) != identity:
        raise ValueError('Checkpoint associé à une autre source ou destination')
    if not marker.exists() and (output / '_delta_log').exists():
        raise ValueError('Table existante sans checkpoint associé : risque de rejeu')
    if not marker.exists():
        with marker.open('x') as stream:
            json.dump(identity, stream)
    raw = (spark.readStream.format('kafka').option('kafka.bootstrap.servers', bootstrap)
           .option('subscribe', topic).option('startingOffsets', 'earliest')
           .option('failOnDataLoss', 'true').option('maxOffsetsPerTrigger', max_offsets).load())
    query = (bronze_frame(raw).writeStream.format('delta').outputMode('append')
             .option('checkpointLocation', str(checkpoint)).trigger(availableNow=True).start(str(output)))
    query.awaitTermination()
    return query


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bootstrap-servers', default='127.0.0.1:9092')
    parser.add_argument('--topic', default='banking.transactions.raw')
    parser.add_argument('--output', default='data/bronze/transactions')
    parser.add_argument('--checkpoint', default='data/checkpoints/bronze-transactions')
    parser.add_argument('--max-offsets', type=int, default=100)
    args = parser.parse_args()
    if args.max_offsets <= 0:
        parser.error('max-offsets doit être positif')
    spark = session()
    spark.sparkContext.setLogLevel('WARN')
    try:
        ingest(spark, args.bootstrap_servers, args.topic, args.output, args.checkpoint, args.max_offsets)
        if not (Path(args.output) / '_delta_log').exists():
            print('Aucun événement disponible ; aucune table créée.')
            return
        table = spark.read.format('delta').load(str(Path(args.output).resolve()))
        table.groupBy('parse_status').count().show()
        print(f'PASS: Bronze Delta contient {table.count()} lignes au total.')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
