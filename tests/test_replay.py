"""Exercise durable CSV progress, interrupted publication and absolute row ranges."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from banking import pipeline, replay


class ReplayTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.csv = self.root / 'source.csv'
        self.csv.write_bytes(Path('tests/fixtures/paysim_synthetic.csv').read_bytes())
        self.snapshot = self.root / 'data/gold/star/_snapshot.json'
        self.cursor = self.root / 'data/checkpoints/csv-replay.json'
        self.checkpoint = self.root / 'data/checkpoints/bronze-transactions/banking-source.json'
        self.checkpoint.parent.mkdir(parents=True)
        self.checkpoint.write_text('{}')
        for name, value in [('ROOT', self.root), ('SHA256', hashlib.sha256(self.csv.read_bytes()).hexdigest())]:
            context = patch.object(pipeline, name, value)
            context.start()
            self.addCleanup(context.stop)
        self.calls = []

    def publish(self, count):
        self.snapshot.parent.mkdir(parents=True, exist_ok=True)
        self.snapshot.write_text(json.dumps({'counts': {'fact_transaction': count}, 'version': count}))

    def run_range(self, csv, limit, rate, offsets, directory, start_row=None):
        self.calls.append((start_row, limit))
        self.publish(limit)
        return 0

    def run_resume(self, target=2, batch_rows=1):
        return replay.resume(self.csv, target, 100, 2, batch_rows, self.root / 'run')

    def test_batches_advance_only_after_validation_and_noop_sends_nothing(self):
        with patch.object(pipeline, 'run_pipeline', side_effect=self.run_range):
            self.assertEqual(self.run_resume(), 0)
            self.assertEqual(self.run_resume(), 0)
        self.assertEqual(self.calls, [(1, 1), (2, 2)])
        state = json.loads(self.cursor.read_text())
        self.assertEqual(state['validated_through'], 2)
        self.assertIsNone(state['pending'])

    def test_bootstrap_verifies_existing_prefix_before_skip(self):
        self.publish(1)
        with patch.object(replay, 'verify') as verify, patch.object(pipeline, 'run_pipeline', side_effect=self.run_range):
            self.assertEqual(self.run_resume(), 0)
        verify.assert_called_once_with(self.csv, 1, self.root / 'run')
        self.assertEqual(self.calls, [(2, 2)])

    def test_failed_bootstrap_cannot_create_cursor_or_publish(self):
        self.publish(1)
        with patch.object(replay, 'verify', side_effect=ValueError('invalid prefix')), patch.object(pipeline, 'run_pipeline') as run:
            self.assertEqual(self.run_resume(), 1)
        run.assert_not_called()
        self.assertFalse(self.cursor.exists())

    def test_interruption_retries_pending_range_then_continues(self):
        self.publish(1)
        with patch.object(replay, 'verify'), patch.object(pipeline, 'run_pipeline', return_value=1):
            self.assertEqual(self.run_resume(target=4, batch_rows=2), 1)
        state = json.loads(self.cursor.read_text())
        self.assertEqual(state['validated_through'], 1)
        self.assertEqual(state['pending'], {'start_row': 2, 'end_row': 3})
        # Simulate tables advanced before the process could commit its CSV cursor.
        self.publish(3)
        with patch.object(pipeline, 'run_pipeline', side_effect=self.run_range):
            self.assertEqual(self.run_resume(target=4, batch_rows=1), 0)
        self.assertEqual(self.calls, [(2, 3), (4, 4)])

    def test_changed_csv_or_snapshot_is_refused(self):
        with patch.object(pipeline, 'run_pipeline', side_effect=self.run_range):
            self.assertEqual(self.run_resume(), 0)
        self.publish(3)
        with patch.object(pipeline, 'run_pipeline') as run:
            self.assertEqual(self.run_resume(target=4), 1)
            run.assert_not_called()
        self.csv.write_text('changed')
        self.assertEqual(self.run_resume(target=4), 1)

    def test_bad_targets_and_missing_checkpoint_are_refused(self):
        self.publish(2)
        with patch.object(replay, 'verify') as verify:
            self.assertEqual(self.run_resume(target=1), 1)
            verify.assert_not_called()
        self.checkpoint.unlink()
        self.assertEqual(self.run_resume(), 1)

    def test_batch_payload_cap_prevents_publication(self):
        item = {'transaction': {'nameOrig': 'C'}, 'large': 'x' * (1024 * 1024)}
        with patch.object(pipeline, 'records', return_value=iter([item] * 129)):
            with self.assertRaisesRegex(ValueError, '128 Mio'):
                pipeline.preflight(self.csv, 129, start_row=1)

    def test_resumed_pipeline_drains_before_producing_and_verifies_total(self):
        commands = {}
        def execute(command, log, env):
            commands[log.stem] = command
            log.write_text('ok')
            return 0
        with patch.object(pipeline, 'preflight'), patch.object(pipeline, 'execute', side_effect=execute):
            self.assertEqual(pipeline.run_pipeline(self.csv, 5, 100, 2, self.root / 'stages', start_row=3), 0)
        self.assertLess(list(commands).index('bronze-drain'), list(commands).index('producer'))
        command = commands['producer']
        self.assertEqual(command[command.index('--start-row') + 1], '3')
        self.assertEqual(command[command.index('--max-messages') + 1], '3')
        command = commands['verify']
        self.assertEqual(command[command.index('--max-messages') + 1], '5')
        self.assertEqual(command[command.index('--csv') + 1], str(self.csv))
