"""Export one published star as four portable Parquet files, verified before publication."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import re
from pathlib import Path
import shutil
import uuid
from banking.bronze import session
from banking.star import register_views, TABLE_KEYS


def export(spark, source, output):
    source, output = Path(source).resolve(), Path(output).resolve()
    if source == output or source in output.parents or output in source.parents:
        raise ValueError('Sortie export distincte de l’étoile et sans imbrication requise')
    if not re.fullmatch(r'[A-Za-z0-9_-]+', output.name):
        raise ValueError('Nom du dossier export limité aux lettres, chiffres, tirets et underscores')
    manifest = register_views(spark, source)
    output.mkdir(parents=True, exist_ok=False)
    spark.conf.set('spark.sql.parquet.outputTimestampType', 'TIMESTAMP_MICROS')
    tables = {}
    for name, primary in TABLE_KEYS.items():
        frame = spark.table(name)
        count = frame.count()
        if count != manifest['counts'][name] or frame.select(primary).distinct().count() != count:
            raise ValueError('Effectif ou clé invalide : ' + name)
        staging = output / ('_writing_' + name)
        frame.coalesce(1).write.mode('error').parquet(str(staging))
        parts = list(staging.glob('part-*.parquet'))
        if len(parts) != 1:
            raise ValueError('Un seul fichier Parquet attendu : ' + name)
        target = output / (name + '.parquet')
        parts[0].replace(target)
        shutil.rmtree(staging)
        restored = spark.read.parquet(str(target)).select(*frame.columns)
        if frame.exceptAll(restored).limit(1).count() or restored.exceptAll(frame).limit(1).count():
            raise ValueError('Export différent de la table Delta : ' + name)
        with target.open('rb') as stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        tables[name] = {'file': target.name, 'rows': count, 'sha256': digest,
                        'bytes': target.stat().st_size, 'schema': frame.schema.jsonValue(),
                        'delta_version': manifest['table_versions'][name]}
    fact = spark.table('fact_transaction')
    from pyspark.sql import functions as F
    totals = fact.agg(F.sum('amount').alias('total_amount'), F.sum('is_fraud').alias('fraud_transactions'),
                     F.sum(F.when(F.col('is_fraud') == 1, F.col('amount')).otherwise(0)).alias('fraud_amount')).first().asDict()
    result = {'format_version': 1, 'export_id': output.name, 'created_at_utc': datetime.now(timezone.utc).isoformat(),
              'silver_version': manifest['silver_version'], 'tables': tables,
              'totals': {k: str(v or 0) for k, v in totals.items()}}
    (output / 'manifest.json').write_text(json.dumps(result, indent=2) + '\n')
    print(f'PASS: Export vérifié : {output}; {tables["fact_transaction"]["rows"]} faits, quatre fichiers Parquet.', flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--star', default='data/gold/star')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    output = args.output or Path('data/exports') / (datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:8])
    spark = session()
    spark.sparkContext.setLogLevel('WARN')
    try:
        export(spark, args.star, output)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
