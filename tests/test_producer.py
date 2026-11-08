import unittest
from pathlib import Path
from banking.producer import event, records

class ProducerTests(unittest.TestCase):
    def test_precision_and_stable_identity(self):
        items = list(records(Path('tests/fixtures/paysim_synthetic.csv'), 'synthetic-v1', 1))
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]['transaction']['amount'], '10.25')
        self.assertEqual(items, list(records(Path('tests/fixtures/paysim_synthetic.csv'), 'synthetic-v1', 1)))
        self.assertNotEqual(items[0]['event_id'], list(records(Path('tests/fixtures/paysim_synthetic.csv'), 'other', 1))[0]['event_id'])

    def test_reject_invalid_values(self):
        row = next(records(Path('tests/fixtures/paysim_synthetic.csv'), 'x', 1))['transaction']
        for key, value in [('amount', 'NaN'), ('amount', '-1'), ('isFraud', '2'), ('type', 'UNKNOWN')]:
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                event({**row, key: value}, 'x', 1)

    def test_resumed_range_preserves_absolute_identity(self):
        source = Path('tests/fixtures/paysim_synthetic.csv')
        full = list(records(source, 'synthetic-v1', 2))
        tail = list(records(source, 'synthetic-v1', 1, start_row=2))
        self.assertEqual(tail, full[1:])
        self.assertEqual(tail[0]['source_row'], 2)
        self.assertEqual(list(records(source, 'synthetic-v1', 1, start_row=3)), [])
        for start, limit in [(0, 1), (1, 0)]:
            with self.assertRaises(ValueError):
                list(records(source, 'synthetic-v1', limit, start_row=start))
