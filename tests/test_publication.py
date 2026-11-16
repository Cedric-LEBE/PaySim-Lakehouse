import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from banking.publication import TABLES, verified_export, publish, import_run


class PublicationTests(unittest.TestCase):
    def fixture(self, parent):
        root=Path(parent)/'sample-export'
        root.mkdir()
        tables={}
        for name in TABLES:
            path=root/(name+'.parquet')
            path.write_bytes(b'fixture')
            tables[name]={'file':path.name,'bytes':7,'sha256':hashlib.sha256(b'fixture').hexdigest()}
        manifest={'format_version':1,'export_id':root.name,'tables':tables}
        (root/'manifest.json').write_text(json.dumps(manifest))
        return root,manifest

    def test_local_corruption_blocks_publication(self):
        with tempfile.TemporaryDirectory() as temporary:
            root,_=self.fixture(temporary)
            verified_export(root)
            (root/'fact_transaction.parquet').write_bytes(b'corrupt')
            with self.assertRaisesRegex(ValueError,'mismatch'):
                verified_export(root)

    def test_manifest_is_written_last_and_objects_are_conditional(self):
        with tempfile.TemporaryDirectory() as temporary:
            root,manifest=self.fixture(temporary)
            puts=[]
            def fake(command):
                if command[2]=='list-objects-v2':return {}
                puts.append(command)
                return {}
            publish(root,manifest,fake)
            self.assertEqual(len(puts),5)
            self.assertTrue(puts[-1][puts[-1].index('--key')+1].endswith('/manifest.json'))
            self.assertTrue(all(c[c.index('--if-none-match')+1]=='*' for c in puts))

    def test_remote_conflict_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as temporary:
            root,manifest=self.fixture(temporary)
            def fake(command):
                if command[2]=='list-objects-v2':
                    return {'Contents':[{'Key':'exports/sample-export/dim_account.parquet'}]}
                if command[2]=='get-object':
                    Path(command[command.index('--key')+2]).write_bytes(b'different')
                    return {}
                self.fail('Unexpected write on conflicting export')
            with self.assertRaisesRegex(ValueError,'differs'):
                publish(root,manifest,fake)

    def test_existing_run_is_monitored_without_triggering_another(self):
        def forbidden(command):self.fail('Run already exists')
        self.assertEqual(import_run({'run_id':123},{},77,'stable-token',forbidden),123)

    def test_submission_passes_identity_token_and_notebook_parameters(self):
        calls=[]
        def fake(command):
            calls.append(command)
            if command[2]=='get':return {'settings':{'tasks':[{'notebook_task':{'notebook_path':'/files/01_import_star.py'}}]}}
            return {'run_id':123}
        state={}
        self.assertEqual(import_run(state,{'export_path':'s3://example/exports/a/'},77,'stable-token',fake),123)
        request=json.loads(calls[-1][-1])
        self.assertEqual(request['idempotency_token'],'stable-token')
        self.assertEqual(state['run_id'],123)
        self.assertEqual(request['job_id'],77)

    def test_published_manifest_with_missing_objects_is_not_repaired(self):
        with tempfile.TemporaryDirectory() as temporary:
            root,manifest=self.fixture(temporary)
            def fake(command):
                if command[2]=='list-objects-v2':
                    return {'Contents':[{'Key':'exports/sample-export/manifest.json'}]}
                if command[2]=='get-object':
                    Path(command[command.index('--key')+2]).write_bytes((root/'manifest.json').read_bytes())
                    return {}
                self.fail('Unexpected repair after manifest publication')
            with self.assertRaisesRegex(ValueError,'missing data'):
                publish(root,manifest,fake)

    def test_unexpected_job_is_not_triggered(self):
        def fake(command):
            self.assertEqual(command[2],'get')
            return {'settings':{'tasks':[{'notebook_task':{'notebook_path':'/unrelated.py'}}]}}
        with self.assertRaisesRegex(ValueError,'expected'):
            import_run({}, {}, 77, 'token', fake)
