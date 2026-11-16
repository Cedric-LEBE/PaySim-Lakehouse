"""Resume a verified export publication with existing AWS/Databricks CLI identities."""
import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile
import time
from banking.replay import save as atomic_save

TABLES={'dim_account','dim_transaction_type','dim_simulation_time','fact_transaction'}
BUCKET='paysim-lakehouse-968077439279-eu-north-1-an'


def save(path, state):
    state['updated_at_utc']=datetime.now(timezone.utc).isoformat()
    atomic_save(path,state)


def cli(command):
    result=subprocess.run(command,capture_output=True,text=True,timeout=300)
    if result.returncode:
        # CLI stderr can include account/authentication details; keep it out of reports.
        raise RuntimeError('CLI operation failed: '+' '.join(command[:3]))
    return json.loads(result.stdout or '{}')


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream,'sha256').hexdigest()


def verified_export(directory):
    directory=Path(directory).resolve()
    manifest_path=directory/'manifest.json'
    if manifest_path.stat().st_size>1024*1024:
        raise ValueError('Manifest too large')
    manifest=json.loads(manifest_path.read_text())
    if manifest.get('format_version')!=1 or not re.fullmatch(r'[A-Za-z0-9_-]+',manifest.get('export_id','')):
        raise ValueError('Invalid export identity')
    if manifest['export_id']!=directory.name or set(manifest['tables'])!=TABLES:
        raise ValueError('Export directory/tables differ from manifest')
    for name,info in manifest['tables'].items():
        path=directory/(name+'.parquet')
        if info['file']!=path.name or path.is_symlink() or path.stat().st_size!=info['bytes'] or digest(path)!=info['sha256']:
            raise ValueError('Export file mismatch: '+name)
    return manifest,digest(manifest_path)


def publish(directory,manifest,call=cli):
    prefix='exports/'+manifest['export_id']+'/'
    existing={o['Key']:o for o in call(['aws','s3api','list-objects-v2','--bucket',BUCKET,'--prefix',prefix,'--region','eu-north-1','--output','json','--no-cli-pager']).get('Contents',[])}
    files=[name+'.parquet' for name in sorted(TABLES)]+['manifest.json']
    unknown=set(existing)-{prefix+name for name in files}
    if unknown:
        raise ValueError('Unexpected objects in export prefix')
    # Check every existing object before writing any missing object. Never overwrite.
    missing=[]
    for name in files:
        local=Path(directory)/name
        key=prefix+name
        if key not in existing:
            missing.append(name)
            continue
        with tempfile.TemporaryDirectory(prefix='paysim-s3-check-') as temporary:
            downloaded=Path(temporary)/'object'
            call(['aws','s3api','get-object','--bucket',BUCKET,'--key',key,str(downloaded),'--region','eu-north-1','--output','json','--no-cli-pager'])
            if downloaded.stat().st_size!=local.stat().st_size or digest(downloaded)!=digest(local):
                raise ValueError('Remote object differs; refusing replacement: '+name)
    if 'manifest.json' not in missing and missing:
        raise ValueError('Published manifest has missing data files; refusing automatic repair')
    for name in missing:
        call(['aws','s3api','put-object','--bucket',BUCKET,'--key',prefix+name,'--body',str(Path(directory)/name),'--if-none-match','*','--region','eu-north-1','--output','json','--no-cli-pager'])
    return 's3://'+BUCKET+'/'+prefix


def import_run(state, parameters, job_id, token, call=cli):
    if 'run_id' not in state:
        job=call(['databricks','api','get','/api/2.2/jobs/get?job_id='+str(job_id)])
        tasks=job.get('settings',{}).get('tasks',[])
        if len(tasks)!=1 or not tasks[0].get('notebook_task',{}).get('notebook_path','').endswith('/01_import_star.py'):
            raise ValueError('Selected job is not the expected single-task import notebook')
        response=call(['databricks','api','post','/api/2.2/jobs/run-now','--json',json.dumps({'job_id':job_id,'notebook_params':parameters,'idempotency_token':token})])
        state['run_id']=response['run_id']
    return state['run_id']


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--export',required=True,type=Path)
    parser.add_argument('--job-id',type=int,default=711893433559744)
    parser.add_argument('--catalog',default='workspace')
    parser.add_argument('--volume',default='workspace.paysim_uploads.s3_exports')
    parser.add_argument('--execute',action='store_true',help='Without this flag, validate and show the plan only')
    args=parser.parse_args()
    if args.job_id < 1:
        parser.error('Job ID must be positive')
    directory=args.export.resolve()
    manifest,sha=verified_export(directory)
    schema='paysim_'+manifest['export_id'].lower().replace('-','_')
    parameters={'export_path':'s3://'+BUCKET+'/exports/'+manifest['export_id']+'/', 'catalog':args.catalog,'s3_volume':args.volume,'target_schema':schema}
    identity={'manifest_sha256':sha,'parameters':parameters,'job_id':args.job_id,'workspace':'https://dbc-22e54698-1ac1.cloud.databricks.com'}
    print(json.dumps({'plan':identity,'execute':args.execute},indent=2),flush=True)
    if not args.execute:
        return 0
    import os
    os.environ['DATABRICKS_HOST']=identity['workspace']
    cli(['databricks','current-user','me'])  # Fail before upload if CLI authentication is unavailable.
    root=Path(__file__).resolve().parents[2]
    state_dir=root/'data/publications'
    state_dir.mkdir(parents=True,exist_ok=True)
    path=state_dir/(manifest['export_id']+'.json')
    with (state_dir/(manifest['export_id']+'.lock')).open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        state=json.loads(path.read_text()) if path.exists() else {'identity':identity,'status':'prepared'}
        if state['identity']!=identity:
            raise ValueError('Publication state belongs to another manifest or destination')
        token=hashlib.sha256(json.dumps(identity,sort_keys=True).encode()).hexdigest()
        try:
            state.update(status='checking_s3')
            save(path,state)
            publish(directory,manifest)
            state.update(status='s3_verified')
            save(path,state)
            run_id=import_run(state,parameters,args.job_id,token)
            state.update(status='import_running')
            save(path,state)
            deadline=time.monotonic()+1200
            while time.monotonic()<deadline:
                run=cli(['databricks','api','get','/api/2.2/jobs/runs/get?run_id='+str(run_id)])
                state['remote_state']=run.get('state',{})
                state['run_page_url']=run.get('run_page_url')
                lifecycle=state['remote_state'].get('life_cycle_state')
                if lifecycle in ('TERMINATED','SKIPPED','INTERNAL_ERROR'):
                    state['status']='complete' if lifecycle=='TERMINATED' and state['remote_state'].get('result_state')=='SUCCESS' else 'import_failed'
                    save(path,state)
                    print(json.dumps(state,indent=2))
                    return 0 if state['status']=='complete' else 1
                save(path,state)
                time.sleep(15)
            state['status']='monitor_timeout'
            save(path,state)
            raise TimeoutError('Remote run may still be active; resume to monitor the same run')
        except BaseException as error:
            state['error_type']=type(error).__name__
            state['failed_phase']=state['status']
            if state['status'] not in ('monitor_timeout','import_failed'):
                state['status']='attention'
                save(path,state)
            raise


if __name__=='__main__':
    raise SystemExit(main())
