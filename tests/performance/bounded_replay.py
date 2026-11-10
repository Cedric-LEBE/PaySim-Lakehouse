"""Bounded, isolated transformation benchmark. No Kafka; never writes production paths."""
import argparse
import json
import os
from pathlib import Path
import platform
import runpy
import sys
import time
import uuid

REPO = Path(__file__).resolve().parents[2]


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--csv', type=Path, default=REPO / 'data/raw/PS_20174392719_1491204439457_log.csv')
    parser.add_argument('--rows', type=int, default=10000)
    parser.add_argument('--batch-size', type=int, default=5000)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    if not 2 <= args.rows <= 50000 or not 1 <= args.batch_size < args.rows:
        parser.error('Use 2–50,000 rows and a batch size between 1 and rows-1.')
    args.csv = args.csv.resolve()
    if not args.csv.is_file():
        parser.error('CSV not found')
    return args


def main():
    args = arguments()
    if args.dry_run:
        print(json.dumps({'rows':args.rows, 'batch_size':args.batch_size,
                          'output_parent':str(REPO / 'data/performance'), 'kafka':False,
                          'full_verification':'after every batch', 'production_writes':False}))
        return
    from pyspark.sql import functions as F
    from banking.bronze import bronze_frame, session
    from banking.producer import records
    from banking.silver import DATASET
    from banking.silver_incremental import build as silver
    from banking.gold_incremental import build as gold
    from banking.star_incremental import build as star

    # Fully validate the bounded input before creating Spark state or any output.
    events = list(records(args.csv, DATASET, args.rows))
    if len(events) != args.rows:
        raise ValueError('CSV shorter than requested prefix')
    root = REPO / 'data/performance' / ('bounded-' + uuid.uuid4().hex)
    root.mkdir(parents=True, exist_ok=False)
    previous = Path.cwd()
    report = {'status':'running','rows':args.rows,'batch_size':args.batch_size,
              'kafka':False,'platform':platform.platform(),'python':platform.python_version(),
              'note':'Fresh process per prefix verification; cold/warm Spark overhead is included. No production checkpoint or table reused.',
              'stages':[]}
    os.chdir(root)
    spark = None
    def stage(name, through, action):
        entry = {'name':name,'through':through,'status':'failed'}
        report['stages'].append(entry)
        start = time.monotonic()
        try:
            action()
            entry['status']='passed'
        finally:
            entry['seconds']=round(time.monotonic()-start,6)
            (root/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    try:
        for start in range(0, args.rows, args.batch_size):
            through = min(start + args.batch_size, args.rows)
            spark = session()
            spark.sparkContext.setLogLevel('WARN')
            def append():
                path = root / f'input-{start+1}-{through}.json'
                with path.open('w') as output:
                    for offset, event in enumerate(events[start:through], start):
                        output.write(json.dumps({'key':event['transaction']['nameOrig'],'value':json.dumps(event),
                            'topic':'performance.isolated','partition':0,'offset':offset,
                            'timestamp':'2026-01-01T00:00:00Z','timestampType':0})+'\n')
                raw = (spark.read.json(str(path)).withColumn('key',F.col('key').cast('binary'))
                       .withColumn('value',F.col('value').cast('binary')).withColumn('timestamp',F.to_timestamp('timestamp')))
                bronze_frame(raw).write.format('delta').mode('append').save(str(root/'data/bronze/transactions'))
            stage('bronze_direct_append',through,append)
            stage('silver',through,lambda:silver(spark,root/'data/bronze/transactions',root/'data/silver',DATASET))
            stage('gold',through,lambda:gold(spark,root/'data/silver/transactions',root/'data/gold'))
            stage('star',through,lambda:star(spark,root/'data/silver/transactions',root/'data/gold/star'))
            spark.stop()
            spark = None
            def verify():
                original = sys.argv
                try:
                    sys.argv = ['replay_check.py','--csv',str(args.csv),'--max-messages',str(through),'--data-root',str(root/'data')]
                    runpy.run_path(str(REPO/'tests/integration/replay_check.py'),run_name='__main__')
                finally:
                    sys.argv = original
            stage('verify',through,verify)
        report['status']='passed'
    except BaseException:
        report['status']='failed'
        raise
    finally:
        try:
            if spark is not None:
                spark.stop()
        finally:
            (root/'report.json').write_text(json.dumps(report,indent=2)+'\n')
            os.chdir(previous)
            print('Benchmark report: '+str(root/'report.json'),flush=True)


if __name__ == '__main__':
    main()
