"""Orchestration failures must stop downstream writes and release Kafka."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from banking import pipeline


class PipelineTests(unittest.TestCase):
    def run_case(self, failure=None):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        directory = Path(temporary.name) / 'run'
        calls = []

        def execute(command, log, env):
            name = log.stem
            calls.append(name)
            log.write_text('simulated failure\n' if name == failure else 'ok\n')
            self.assertNotIn('BANKING_SPARK_APP', env)
            return 1 if name == failure else 0

        with patch.object(pipeline, 'preflight'), patch.object(pipeline, 'execute', side_effect=execute):
            code = pipeline.run_pipeline(Path('source.csv'), 10000, 100, 1000, directory)
        return code, calls, json.loads((directory / 'report.json').read_text())

    def test_success_stops_kafka_before_silver_and_verifies_last(self):
        code, calls, report = self.run_case()
        self.assertEqual(code, 0)
        self.assertEqual(calls, ['kafka-start', 'kafka-topic', 'producer', 'bronze',
                                 'kafka-stop', 'silver', 'gold', 'star', 'verify'])
        self.assertEqual(report['status'], 'passed')
        self.assertTrue(all(stage['seconds'] >= 0 for stage in report['stages']))

    def test_bronze_failure_prevents_silver_and_cleans_up(self):
        code, calls, report = self.run_case('bronze')
        self.assertEqual(code, 1)
        self.assertEqual(calls[-1], 'kafka-cleanup')
        self.assertNotIn('silver', calls)
        self.assertEqual(report['status'], 'failed')

    def test_partial_start_failure_also_cleans_up(self):
        code, calls, _ = self.run_case('kafka-start')
        self.assertEqual(code, 1)
        self.assertEqual(calls, ['kafka-start', 'kafka-cleanup'])

    def test_bad_csv_hash_never_starts_kafka(self):
        with tempfile.TemporaryDirectory() as temporary:
            csv = Path(temporary) / 'bad.csv'
            csv.write_text('invalid')
            directory = Path(temporary) / 'run'
            with patch.object(pipeline, 'execute') as execute:
                code = pipeline.run_pipeline(csv, 10000, 100, 1000, directory)
            execute.assert_not_called()
            self.assertEqual(code, 1)
            self.assertIn('Empreinte CSV incorrecte', json.loads((directory / 'report.json').read_text())['error'])


if __name__ == '__main__':
    unittest.main()
