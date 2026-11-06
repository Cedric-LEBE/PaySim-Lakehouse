"""Check star grain, role joins, stable keys and interrupted snapshot publication."""
import json
import uuid
from pathlib import Path
from unittest.mock import patch
from pyspark.sql import functions as F
from banking.bronze import session
from banking.star import build, register_views

root = Path('data/integration/star-' + uuid.uuid4().hex).resolve()
spark = session()
spark.sparkContext.setLogLevel('WARN')
source = Path('data/silver/transactions').resolve()
star = root / 'star'

try:
    first = build(spark, source, star)
    register_views(spark, star)
    expected = spark.read.format('delta').load(str(source))
    fact = spark.table('fact_transaction')
    assert fact.count() == expected.count() == 100
    assert fact.select('transaction_key').distinct().count() == 100
    original_keys = {r.account_id: r.account_key for r in spark.table('dim_account').collect()}
    original_times = {r.simulation_step: r.simulation_time_key for r in spark.table('dim_simulation_time').collect()}
    recovered = spark.sql('''SELECT f.transaction_key, d.transaction_type AS type,
          t.simulation_step AS step, o.account_id AS nameOrig, r.account_id AS nameDest,
          f.amount, f.is_fraud AS isFraud, f.is_flagged_fraud AS isFlaggedFraud
        FROM fact_transaction f
        JOIN dim_transaction_type d ON f.transaction_type_key=d.transaction_type_key
        JOIN dim_simulation_time t ON f.simulation_time_key=t.simulation_time_key
        JOIN dim_account o ON f.origin_account_key=o.account_key
        JOIN dim_account r ON f.destination_account_key=r.account_key''')
    original = expected.select(F.col('event_id').alias('transaction_key'), 'type', 'step',
                               'nameOrig', 'nameDest', 'amount', 'isFraud', 'isFlaggedFraud')
    assert recovered.count() == 100 and recovered.exceptAll(original).count() == 0
    assert original.exceptAll(recovered).count() == 0
    query = Path('sql/star/01_summary.sql').read_text()
    summary = spark.sql(query).first()
    assert summary.transactions == 100 and summary.fraud_transactions == 2
    assert str(summary.total_amount) == '11644940.90' and str(summary.fraud_amount) == '362.00'
    for path in sorted(Path('sql/star').glob('*.sql')):
        spark.sql(path.read_text()).collect()
    repeated = build(spark, source, star)
    assert repeated['counts'] == first['counts']
    register_views(spark, star)
    assert {r.account_id: r.account_key for r in spark.table('dim_account').collect()} == original_keys
    saved_manifest = json.loads((star / '_snapshot.json').read_text())
    extra = (expected.orderBy('source_row').limit(1).withColumn('source_row', F.lit(100001).cast('long'))
             .withColumn('event_id', F.sha2(F.concat_ws(':', 'dataset_id', F.col('source_row').cast('string')), 256))
             .withColumn('step', F.lit(25).cast('long'))
             .withColumn('nameOrig', F.lit('C_STAR_TEST')).withColumn('nameDest', F.lit('C_STAR_TEST')))
    expanded = root / 'expanded-silver'
    expected.unionByName(extra).write.format('delta').save(str(expanded))
    try:
        with patch('pathlib.Path.replace', side_effect=OSError('publication interrompue')):
            build(spark, expanded, star)
    except OSError:
        pass
    else:
        raise AssertionError('Interruption simulée ignorée')
    assert json.loads((star / '_snapshot.json').read_text()) == saved_manifest
    register_views(spark, star)
    assert spark.table('fact_transaction').count() == 100  # old complete versions remain readable
    final = build(spark, expanded, star)
    register_views(spark, star)
    assert final['counts']['fact_transaction'] == 101
    updated = {r.account_id: r.account_key for r in spark.table('dim_account').collect()}
    assert all(updated[account] == key for account, key in original_keys.items())
    assert all({r.simulation_step: r.simulation_time_key for r in spark.table('dim_simulation_time').collect()}[step] == key
               for step, key in original_times.items())
    new_fact = spark.table('fact_transaction').filter('source_row = 100001').first()
    assert new_fact.origin_account_key == new_fact.destination_account_key == updated['C_STAR_TEST']
    hour = spark.table('dim_simulation_time').filter('simulation_step = 25').first()
    assert hour.simulation_day_index == 2 and hour.hour_index_in_simulated_day == 0
    duplicated = root / 'duplicated-silver'
    expected.unionByName(expected.limit(1)).write.format('delta').save(str(duplicated))
    try:
        build(spark, duplicated, root / 'invalid-star')
    except ValueError:
        assert not (root / 'invalid-star/_snapshot.json').exists()
    else:
        raise AssertionError('Doublon accepté')
    print('PASS: Étoile : grain, SQL, deux rôles compte, clés stables et publication interrompue vérifiés.', flush=True)
finally:
    spark.stop()
