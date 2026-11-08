# Databricks notebook source
# Deploy this notebook together with the sibling _import_support.py workspace file.
# Source: four Parquet files and manifest.json in a UC volume or S3 export directory.
# S3 URLs require an existing external volume; no AWS keys or local copies are used.
import re
from decimal import Decimal
from _import_support import PRIMARY_KEYS, quote, resolve_export_path, load_manifest, verify_file
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, DecimalType

dbutils.widgets.text('export_path', '')
dbutils.widgets.text('catalog', spark.catalog.currentCatalog())
dbutils.widgets.text('target_schema', '')
dbutils.widgets.text('s3_volume', '')
export_path = resolve_export_path(
    dbutils.widgets.get('export_path').strip(),
    dbutils.widgets.get('s3_volume').strip(),
    lambda name: spark.sql('DESCRIBE VOLUME ' + name).first().asDict()
)
catalog = dbutils.widgets.get('catalog')
manifest = load_manifest(export_path)
schema = dbutils.widgets.get('target_schema').strip() or 'paysim_' + manifest['export_id'].lower().replace('-', '_')
assert re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', schema)
namespace = quote(catalog) + '.' + quote(schema)
primary_keys = PRIMARY_KEYS
# Verify every file before starting Spark validation or any target writes.
for name, info in manifest['tables'].items():
    verify_file(export_path / info['file'], info)
frames = {}
for name, primary in primary_keys.items():
    info = manifest['tables'][name]
    path = export_path / info['file']
    # Explicit source types preserve monetary Decimal(20,2).
    frame = spark.read.schema(StructType.fromJson(info['schema'])).parquet(str(path))
    stats = frame.agg(F.count('*').alias('rows'), F.countDistinct(primary).alias('ids')).first()
    assert stats.rows == stats.ids == info['rows'], name
    frames[name] = frame
fact = frames['fact_transaction']
assert fact.schema['amount'].dataType == DecimalType(20, 2)
for fk, dim, pk in [('transaction_type_key', 'dim_transaction_type', 'transaction_type_key'),
                    ('simulation_time_key', 'dim_simulation_time', 'simulation_time_key'),
                    ('origin_account_key', 'dim_account', 'account_key'),
                    ('destination_account_key', 'dim_account', 'account_key')]:
    assert fact.join(frames[dim], fact[fk] == frames[dim][pk], 'left_anti').limit(1).count() == 0, fk
stats = fact.agg(F.sum('amount').alias('amount'), F.sum('is_fraud').alias('frauds'),
                 F.sum(F.when(F.col('is_fraud') == 1, F.col('amount')).otherwise(0)).alias('fraud_amount')).first()
assert (stats.amount or 0) == Decimal(manifest['totals']['total_amount'])
assert (stats.frauds or 0) == int(manifest['totals']['fraud_transactions'])
assert (stats.fraud_amount or 0) == Decimal(manifest['totals']['fraud_amount'])

# COMMAND ----------
# A new schema per export prevents accidental replacement of a previous dashboard snapshot.
# Resume interrupted imports, validating existing tables instead of overwriting them.
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {namespace}')
for name, frame in frames.items():
    target = namespace + '.' + quote(name)
    if spark.catalog.tableExists(target):
        existing = spark.table(target)
        assert [(f.name, f.dataType) for f in existing.schema] == [(f.name, f.dataType) for f in frame.schema], target
        assert existing.exceptAll(frame).limit(1).count() == 0, target
        assert frame.exceptAll(existing).limit(1).count() == 0, target
    else:
        temporary_view = '_paysim_import_' + name
        frame.createOrReplaceTempView(temporary_view)
        spark.sql(f'CREATE TABLE {target} USING DELTA AS SELECT * FROM {quote(temporary_view)}')
spark.sql(f'''CREATE OR REPLACE VIEW {namespace}.transactions_dashboard AS
SELECT f.*, ty.transaction_type, tm.simulation_step, tm.simulation_day_index,
       tm.hour_index_in_simulated_day, orig.account_id AS origin_account,
       dest.account_id AS destination_account
FROM {namespace}.fact_transaction f
JOIN {namespace}.dim_transaction_type ty ON f.transaction_type_key = ty.transaction_type_key
JOIN {namespace}.dim_simulation_time tm ON f.simulation_time_key = tm.simulation_time_key
JOIN {namespace}.dim_account orig ON f.origin_account_key = orig.account_key
JOIN {namespace}.dim_account dest ON f.destination_account_key = dest.account_key''')
assert spark.table(namespace + '.transactions_dashboard').count() == manifest['tables']['fact_transaction']['rows']
print('PASS: Tables importées et vue dashboard validée : ' + namespace + '.transactions_dashboard')
print('Instantané exporté à : ' + manifest['created_at_utc'] + ' ; version Silver : ' + str(manifest['silver_version']))
# Paste this fully qualified view name into the dashboard SQL files instead of {{STAR_VIEW}}.
