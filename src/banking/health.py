"""Read-only local pipeline health check: no Spark, Kafka or cloud credentials."""
import argparse
import fcntl
import json
from pathlib import Path


def _inspect_unlocked(root, target):
    root = Path(root).resolve()
    result = {'status':'unknown', 'target_rows':target, 'validated_through':None,
              'pending':None, 'latest_report':None, 'issues':[],
              'scope':'local metadata only; S3 and Databricks are not checked'}
    cursor = root / 'data/checkpoints/csv-replay.json'
    snapshot = root / 'data/gold/star/_snapshot.json'
    if not cursor.exists() or not snapshot.exists():
        result['issues'].append('Replay cursor or star manifest is missing')
        return result
    try:
        state = json.loads(cursor.read_text())
        published = json.loads(snapshot.read_text())
        through = state['validated_through']
        if type(through) is not int or through < 0:
            raise ValueError('Invalid validated row count')
        result.update(validated_through=through, pending=state.get('pending'))
        reports = list((root/'data/runs').rglob('report.json'))
        if reports:
            latest = max(reports, key=lambda path:path.stat().st_mtime_ns)
            report = json.loads(latest.read_text())
            result['latest_report']={'path':str(latest.relative_to(root)), 'status':report.get('status'),
                                     'failed_stages':[s['name'] for s in report.get('stages',[]) if s.get('exit_code',0) != 0]}
            if report.get('status') != 'passed':
                result['issues'].append('Latest pipeline report is not passed')
        if state.get('pending') is not None:
            result['issues'].append('Pending batch requires resume; do not edit the cursor')
        elif published != state.get('snapshot') or published['counts']['fact_transaction'] != through:
            result['issues'].append('Published star manifest differs from validated cursor')
        result['status'] = 'attention' if result['issues'] else ('complete' if through >= target else 'ready_to_resume')
    except (OSError, ValueError, KeyError, TypeError) as error:
        result['status']='attention'
        result['issues'].append('Invalid or unreadable metadata: '+str(error))
    return result


def inspect(root, target):
    root = Path(root).resolve()
    lock_path = root / 'data/runs/pipeline.lock'
    if not lock_path.exists():
        return _inspect_unlocked(root, target)
    with lock_path.open('r') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            return {'status':'running', 'target_rows':target, 'validated_through':None,
                    'pending':None, 'latest_report':None, 'issues':[],
                    'scope':'pipeline lock held; progress and remote destinations not checked'}
        try:
            return _inspect_unlocked(root, target)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument('--target', type=int, default=6362620)
    args=parser.parse_args()
    if args.target < 1:
        parser.error('Target must be positive')
    result=inspect(args.root,args.target)
    print(json.dumps(result,indent=2))
    return 0 if result['status'] in ('complete','running') else 1


if __name__=='__main__':
    raise SystemExit(main())
