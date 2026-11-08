"""Gate a manual deployment on explicit configuration and CI for the exact main SHA."""
import argparse
import json
import os
import re
import time
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

EXPECTED_JOBS = {'Unit tests (Python 3.13)', 'Incremental pipeline (Spark / Delta)'}


def check_config(env):
    if env.get('GITHUB_EVENT_NAME') != 'workflow_dispatch' or env.get('GITHUB_REF') != 'refs/heads/main':
        raise ValueError('Deployment requires a manual dispatch from main.')
    if env.get('DATABRICKS_CD_ENABLED') != 'true':
        raise ValueError('CD is not enabled. Configure supported OIDC federation or explicitly opt into the PAT secret route before enabling DATABRICKS_CD_ENABLED.')
    mode = env.get('DATABRICKS_AUTH_TYPE') or 'github-oidc'
    if mode not in ('github-oidc', 'pat'):
        raise ValueError('Only github-oidc or an explicitly configured pat mode is supported.')
    if mode == 'pat' and env.get('DATABRICKS_PAT_READY') != 'true':
        raise ValueError('PAT mode requires explicit DATABRICKS_PAT_READY=true and the protected environment secret.')
    required = ['DATABRICKS_HOST', 'DATABRICKS_WAREHOUSE_ID']
    if mode == 'github-oidc':
        required.append('DATABRICKS_CLIENT_ID')
    for key in required:
        if not env.get(key):
            raise ValueError('Missing repository variable: ' + key)
    host = urlparse(env['DATABRICKS_HOST'])
    if host.scheme != 'https' or not host.hostname or host.username or host.password or host.query or host.fragment or host.path not in ('', '/'):
        raise ValueError('DATABRICKS_HOST must be an HTTPS workspace origin without credentials or URL parameters.')
    if mode == 'github-oidc' and not re.fullmatch(r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}', env['DATABRICKS_CLIENT_ID']):
        raise ValueError('Invalid Databricks application ID.')
    if not re.fullmatch(r'[0-9a-fA-F]+', env['DATABRICKS_WAREHOUSE_ID']):
        raise ValueError('Invalid SQL warehouse ID.')


def api_get(path):
    request = Request('https://api.github.com' + path, headers={
        'Authorization': 'Bearer ' + os.environ['GITHUB_TOKEN'],
        'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28',
        'User-Agent': 'PaySim-Lakehouse-deployment'})
    with urlopen(request, timeout=30) as response:
        return json.load(response)


def check_ci(repo, sha, get=api_get):
    if get(f'/repos/{repo}/git/ref/heads/main')['object']['sha'] != sha:
        raise ValueError('Main has moved. Dispatch a new deployment for the current main commit.')
    query = urlencode({'branch': 'main', 'event': 'push', 'head_sha': sha, 'per_page': 100})
    runs = get(f'/repos/{repo}/actions/workflows/ci.yml/runs?{query}')['workflow_runs']
    runs = [r for r in runs if r.get('head_sha') == sha and r.get('head_branch') == 'main' and r.get('event') == 'push']
    if not runs:
        return False
    run = max(runs, key=lambda r: r['run_number'])
    if run['status'] != 'completed':
        return False
    if run.get('conclusion') != 'success':
        raise ValueError('The latest CI run for this commit did not succeed.')
    jobs = []
    page = 1
    while True:
        batch = get(f'/repos/{repo}/actions/runs/{run["id"]}/jobs?filter=latest&per_page=100&page={page}')['jobs']
        jobs.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    passed = {j['name'] for j in jobs if j.get('conclusion') == 'success'}
    if not EXPECTED_JOBS.issubset(passed):
        raise ValueError('Both expected CI jobs must succeed; skipped jobs do not qualify.')
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--timeout', type=int, default=900)
    args = parser.parse_args()
    try:
        check_config(os.environ)
        deadline = time.monotonic() + args.timeout
        while True:
            if check_ci(os.environ['GITHUB_REPOSITORY'], os.environ['GITHUB_SHA']):
                print('PASS: explicit CD configuration and successful CI for the exact main commit.')
                return 0
            if time.monotonic() >= deadline:
                raise ValueError('Timed out waiting for successful CI on this exact commit.')
            print('Waiting for CI on the selected main commit...', flush=True)
            time.sleep(15)
    except Exception as error:
        # Never dump authentication environment variables or raw credential configuration.
        print('Deployment blocked: ' + str(error), flush=True)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
