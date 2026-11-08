"""Verify portable export, non-overwrite behavior and dashboard SQL on synthetic data."""
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import uuid
from pyspark.sql import functions as F
from banking.bronze import session
from banking.producer import records
from banking.silver import MONEY
from banking.star import build
from banking.export_star import export

root = Path('data/integration/export-' + uuid.uuid4().hex).resolve()
spark = session()
spark.sparkContext.setLogLevel('WARN')
try:
    events = list(records(Path('tests/fixtures/paysim_synthetic.csv'), 'export-synthetic', 2))
    rows = [{**e['transaction'], 'event_id': e['event_id'], 'dataset_id': e['dataset_id'],
             'source_row': e['source_row'], 'bronze_version': 1, 'topic': 'export.synthetic',
             'partition': 0, 'offset': e['source_row'], 'kafka_timestamp': '2026-01-01T00:00:00Z',
             'ingested_at': '2026-01-01T00:00:00Z'} for e in events]
    source = spark.createDataFrame(rows)
    for name in MONEY:
        source = source.withColumn(name, F.col(name).cast('decimal(20,2)'))
    for name in ['kafka_timestamp', 'ingested_at']:
        source = source.withColumn(name, F.to_timestamp(name))
    source.write.format('delta').save(str(root / 'silver'))
    star = build(spark, root / 'silver', root / 'star')
    manifest = export(spark, root / 'star', root / 'export-A')
    assert manifest['silver_version'] == star['silver_version']
    assert manifest['tables']['fact_transaction']['rows'] == 2
    for name, info in manifest['tables'].items():
        path = root / 'export-A' / info['file']
        with path.open('rb') as stream:
            assert hashlib.file_digest(stream, 'sha256').hexdigest() == info['sha256']
        spark.read.parquet(str(path)).createOrReplaceTempView(name)
    spark.sql('''SELECT f.*, ty.transaction_type, tm.simulation_step,
                 tm.simulation_day_index, tm.hour_index_in_simulated_day
                 FROM fact_transaction f JOIN dim_transaction_type ty
                 ON f.transaction_type_key=ty.transaction_type_key
                 JOIN dim_simulation_time tm ON f.simulation_time_key=tm.simulation_time_key''').createOrReplaceTempView('dashboard_export_check')
    results = {}
    for query in sorted(Path('sql/dashboards').glob('*.sql')):
        results[query.name] = spark.sql(query.read_text().replace('{{STAR_VIEW}}', 'dashboard_export_check')).collect()
    summary = results['01_activity_summary.sql'][0]
    assert summary.transactions == 2 and summary.total_amount == Decimal('60.25')
    assert summary.fraud_transactions == 1 and summary.fraud_amount == Decimal('50.00')
    assert sum(r.transactions for r in results['02_activity_by_type.sql']) == 2
    assert sum(r.transactions for r in results['03_activity_by_hour.sql']) == 2
    before = (root / 'export-A/manifest.json').read_bytes()
    try:
        export(spark, root / 'star', root / 'export-A')
    except FileExistsError:
        pass
    else:
        raise AssertionError('Export existant écrasé')
    assert (root / 'export-A/manifest.json').read_bytes() == before
    print('PASS: Export synthétique vérifié, empreintes, refus d’écrasement et trois requêtes dashboard validés.', flush=True)
finally:
    spark.stop()
