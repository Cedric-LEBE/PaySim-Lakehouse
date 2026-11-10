"""Prevent deployments for stale commits, failed/skipped CI or incomplete auth setup."""
import importlib.util
from pathlib import Path
import unittest

spec=importlib.util.spec_from_file_location('deploy_gate',Path(__file__).resolve().parents[1]/'scripts/ci/check_deploy_ready.py')
gate=importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)

class DeployGateTests(unittest.TestCase):
    def configured(self):
        return {'GITHUB_EVENT_NAME':'workflow_dispatch','GITHUB_REF':'refs/heads/main',
                'DATABRICKS_CD_ENABLED':'true','DATABRICKS_HOST':'https://example.cloud.databricks.com',
                'DATABRICKS_CLIENT_ID':'12345678-1234-1234-1234-123456789abc','DATABRICKS_WAREHOUSE_ID':'abc123'}

    def test_unconfigured_free_edition_route_is_explicitly_blocked(self):
        env=self.configured();env.pop('DATABRICKS_CD_ENABLED')
        with self.assertRaisesRegex(ValueError,'not enabled'):gate.check_config(env)

    def test_configuration_rejects_other_events_refs_and_credential_urls(self):
        for change in [{'GITHUB_EVENT_NAME':'pull_request'}, {'GITHUB_REF':'refs/heads/feature'},
                       {'DATABRICKS_HOST':'https://user:password@example.com'}, {'DATABRICKS_CLIENT_ID':''}]:
            with self.subTest(change=change),self.assertRaises(ValueError):gate.check_config({**self.configured(),**change})
        gate.check_config(self.configured())

    def test_pat_is_an_explicit_choice_and_never_an_automatic_fallback(self):
        env={**self.configured(),'DATABRICKS_AUTH_TYPE':'pat'}
        env.pop('DATABRICKS_CLIENT_ID')
        with self.assertRaisesRegex(ValueError,'explicit'):gate.check_config(env)
        gate.check_config({**env,'DATABRICKS_PAT_READY':'true'})

    def test_unknown_authentication_mode_is_rejected(self):
        with self.assertRaisesRegex(ValueError,'Only'):gate.check_config({**self.configured(),'DATABRICKS_AUTH_TYPE':'oauth-u2m'})

    def ci(self, runs=None, jobs=None, head='abc'):
        if runs is None:runs=[{'id':10,'run_number':1,'head_sha':'abc','head_branch':'main','event':'push','status':'completed','conclusion':'success'}]
        if jobs is None:jobs=[{'name':name,'conclusion':'success'} for name in gate.EXPECTED_JOBS]
        def get(path):
            if '/git/ref/' in path:return {'object':{'sha':head}}
            if '/workflows/' in path:return {'workflow_runs':runs}
            return {'jobs':jobs}
        return get

    def test_exact_commit_and_all_jobs_are_required(self):
        self.assertTrue(gate.check_ci('owner/repo','abc',self.ci()))
        with self.assertRaisesRegex(ValueError,'moved'):gate.check_ci('owner/repo','abc',self.ci(head='def'))
        with self.assertRaisesRegex(ValueError,'All expected'):gate.check_ci('owner/repo','abc',self.ci(jobs=[{'name':n,'conclusion':'skipped'} for n in gate.EXPECTED_JOBS]))

    def test_no_run_or_running_run_waits_instead_of_deploying(self):
        self.assertFalse(gate.check_ci('owner/repo','abc',self.ci(runs=[])))
        run={'id':10,'run_number':1,'head_sha':'abc','head_branch':'main','event':'push','status':'in_progress'}
        self.assertFalse(gate.check_ci('owner/repo','abc',self.ci(runs=[run])))
        self.assertFalse(gate.check_ci('owner/repo','abc',self.ci(runs=[{**run,'head_sha':'another'}])))

    def test_newer_failed_run_cannot_fall_back_to_older_success(self):
        base={'head_sha':'abc','head_branch':'main','event':'push','status':'completed'}
        runs=[{**base,'id':10,'run_number':1,'conclusion':'success'}, {**base,'id':20,'run_number':2,'conclusion':'failure'}]
        with self.assertRaisesRegex(ValueError,'did not succeed'):gate.check_ci('owner/repo','abc',self.ci(runs=runs))

if __name__=='__main__':unittest.main()
