import fcntl
import json
from pathlib import Path
import tempfile
import unittest
from banking.health import inspect


class HealthTests(unittest.TestCase):
    def fixture(self, root, pending=None):
        manifest={'counts':{'fact_transaction':10},'silver_version':2}
        state={'validated_through':10,'pending':pending,'snapshot':manifest}
        for name, value in [('data/checkpoints/csv-replay.json',state),('data/gold/star/_snapshot.json',manifest)]:
            path=Path(root)/name
            path.parent.mkdir(parents=True,exist_ok=True)
            path.write_text(json.dumps(value))

    def test_validated_prefix_is_complete_or_resumable(self):
        with tempfile.TemporaryDirectory() as root:
            self.fixture(root)
            self.assertEqual(inspect(root,10)['status'],'complete')
            self.assertEqual(inspect(root,20)['status'],'ready_to_resume')

    def test_pending_or_manifest_drift_requires_attention(self):
        with tempfile.TemporaryDirectory() as root:
            self.fixture(root,{'start_row':11,'end_row':20})
            self.assertEqual(inspect(root,20)['status'],'attention')
            self.fixture(root)
            path=Path(root)/'data/gold/star/_snapshot.json'
            path.write_text('{"counts":{"fact_transaction":11}}')
            self.assertEqual(inspect(root,10)['status'],'attention')

    def test_missing_metadata_is_unknown_and_does_not_create_files(self):
        with tempfile.TemporaryDirectory() as root:
            self.assertEqual(inspect(root,10)['status'],'unknown')
            self.assertEqual(list(Path(root).iterdir()),[])

    def test_active_pipeline_is_not_reported_as_failed(self):
        with tempfile.TemporaryDirectory() as root:
            path=Path(root)/'data/runs/pipeline.lock'
            path.parent.mkdir(parents=True)
            with path.open('w') as lock:
                fcntl.flock(lock,fcntl.LOCK_EX | fcntl.LOCK_NB)
                self.assertEqual(inspect(root,10)['status'],'running')
