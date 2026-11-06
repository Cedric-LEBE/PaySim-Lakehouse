import io
import json
import unittest
from pathlib import Path
from unittest.mock import Mock
from banking.consumer import decode, process
from banking.producer import records

class ConsumerTests(unittest.TestCase):
    def setUp(self):
        self.item = next(records(Path('tests/fixtures/paysim_synthetic.csv'), 'test', 1))
        self.message = Mock()
        self.message.value.return_value = json.dumps(self.item).encode()
        self.message.key.return_value = b'C_SYNTH_1'
        self.message.topic.return_value = 'test'
        self.message.partition.return_value = 0
        self.message.offset.return_value = 42

    def test_commit_after_output(self):
        client = Mock()
        output = io.StringIO()
        def committed(**kwargs):
            self.assertEqual(json.loads(output.getvalue())['offset'], 42)
        client.commit.side_effect = committed
        process(client, self.message, output)
        client.commit.assert_called_once_with(message=self.message, asynchronous=False)

    def test_invalid_message_is_not_committed(self):
        for payload in [b'bad json', b'null', json.dumps({**self.item, 'event_id': 'wrong'}).encode()]:
            client = Mock()
            self.message.value.return_value = payload
            with self.assertRaises(ValueError):
                process(client, self.message, io.StringIO())
            client.commit.assert_not_called()

    def test_output_failure_is_not_committed(self):
        client = Mock()
        output = Mock()
        output.write.side_effect = OSError('disk full')
        with self.assertRaises(OSError):
            process(client, self.message, output)
        client.commit.assert_not_called()

    def test_wrong_key_is_not_committed(self):
        client = Mock()
        self.message.key.return_value = b'other'
        with self.assertRaises(ValueError):
            process(client, self.message, io.StringIO())
        client.commit.assert_not_called()
