"""Execute the deployed import notebook on an isolated export, no cloud credentials.

Actual Spark/Delta imports and retries use local files. S3 metadata and the FUSE
mount are emulated; the integration does not establish live S3 connectivity.
"""
from decimal import Decimal
import json
from pathlib import Path
import runpy
import sys
import uuid
from unittest.mock import patch

from pyspark.sql import functions as F
from banking.bronze import session
from banking.producer import records
from banking.silver import MONEY
from banking.star import build
from banking.export_star import export

sys.path.insert(0, str(Path('databricks/notebooks').resolve()))
import _import_support as support

root = Path('data/integration/import-' + uuid.uuid4().hex).resolve()
schema = 'import_check_' + uuid.uuid4().hex
spark = session()
spark.sparkContext.setLogLevel('WARN')
notebook = Path('databricks/notebooks/01_import_star.py')


class Widgets:
    def __init__(self, values):
        self.values = values
    def text(self, name, default):
        self.values.setdefault(name, default)
    def get(self, name):
        return self.values[name]


class Utils:
    def __init__(self, values):
        self.widgets = Widgets(values)


original_resolver = support.resolve_export_path
source_url = 's3://synthetic-import/exports/export-A'
volume_name = 'workspace.synthetic.import_source'


def resolve(value, volume, describe):
    if value.startswith('s3://'):
        # Execute the real confinement logic, then map the emulated FUSE path.
        resolved = original_resolver(value, volume, lambda _: {
            'volumeType': 'EXTERNAL', 'storageLocation': 's3://synthetic-import/exports'})
        assert resolved == Path('/Volumes/workspace/synthetic/import_source/export-A')
    else:
        assert original_resolver(value, volume, describe) == Path('/Volumes/workspace/synthetic/import_source/export-A')
    return root / 'export-A'


def run(source=source_url):
    values = {'export_path': source, 's3_volume': volume_name,
              'catalog': 'spark_catalog', 'target_schema': schema}
    with patch.object(support, 'resolve_export_path', side_effect=resolve):
        return runpy.run_path(str(notebook), init_globals={'spark': spark, 'dbutils': Utils(values)})


try:
    events = list(records(Path('tests/fixtures/paysim_synthetic.csv'), 'import-synthetic', 2))
    rows = [{**item['transaction'], 'event_id': item['event_id'], 'dataset_id': item['dataset_id'],
             'source_row': item['source_row'], 'bronze_version': 1, 'topic': 'import.synthetic',
             'partition': 0, 'offset': item['source_row'], 'kafka_timestamp': '2026-01-01T00:00:00Z',
             'ingested_at': '2026-01-01T00:00:00Z'} for item in events]
    source = spark.createDataFrame(rows)
    for name in MONEY:
        source = source.withColumn(name, F.col(name).cast('decimal(20,2)'))
    for name in ['kafka_timestamp', 'ingested_at']:
        source = source.withColumn(name, F.to_timestamp(name))
    source.write.format('delta').save(str(root / 'silver'))
    build(spark, root / 'silver', root / 'star')
    export(spark, root / 'star', root / 'export-A')
    # A unique test database with explicitly isolated managed-table storage.
    spark.sql(f"CREATE SCHEMA spark_catalog.{schema} LOCATION '{root / 'managed'}'")
    manifest_path = root / 'export-A/manifest.json'
    original = manifest_path.read_bytes()
    damaged = json.loads(original)
    damaged['tables']['dim_account']['sha256'] = '0' * 64
    manifest_path.write_text(json.dumps(damaged))
    try:
        run()
    except ValueError as error:
        assert 'SHA-256' in str(error)
    else:
        raise AssertionError('Corruption accepted')
    assert not spark.catalog.tableExists(f'spark_catalog.{schema}.dim_transaction_type')
    damaged = json.loads(original)
    damaged['totals']['total_amount'] = '999.99'
    manifest_path.write_text(json.dumps(damaged))
    try:
        run()
    except AssertionError:
        pass
    else:
        raise AssertionError('Incorrect totals accepted')
    assert not spark.catalog.tableExists(f'spark_catalog.{schema}.dim_transaction_type')
    manifest_path.write_bytes(original)
    run()
    target = f'spark_catalog.{schema}.fact_transaction'
    before = spark.sql(f'DESCRIBE HISTORY {target} LIMIT 1').first().version
    run('/Volumes/workspace/synthetic/import_source/export-A')
    assert spark.sql(f'DESCRIBE HISTORY {target} LIMIT 1').first().version == before
    view = spark.table(f'spark_catalog.{schema}.transactions_dashboard')
    stats = view.agg(F.count('*').alias('rows'), F.sum('amount').alias('amount'), F.sum('is_fraud').alias('frauds')).first()
    assert (stats.rows, stats.amount, stats.frauds) == (2, Decimal('60.25'), 1)
    # Existing mismatched targets must be refused, never overwritten.
    spark.sql(f'UPDATE {target} SET amount = amount + 1')
    try:
        run()
    except AssertionError:
        pass
    else:
        raise AssertionError('Conflicting existing target accepted')
    assert spark.table(target).agg(F.sum('amount')).first()[0] == Decimal('62.25')
    print('PASS: notebook import réel, S3/Volumes simulés, SHA-256 avant écriture, Decimal, références, totaux, relance sans écrasement et conflit refusé.', flush=True)
finally:
    spark.sql(f'DROP SCHEMA IF EXISTS spark_catalog.{schema} CASCADE')
    spark.stop()
