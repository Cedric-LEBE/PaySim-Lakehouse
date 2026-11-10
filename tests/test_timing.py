import contextlib
import io
import json
import unittest
from unittest.mock import patch
from banking.timing import measure


class TimingTests(unittest.TestCase):
    def test_success_is_recorded_with_elapsed_time(self):
        output = io.StringIO()
        with patch('banking.timing.time.monotonic', side_effect=[10, 12.5]), contextlib.redirect_stderr(output):
            with measure('verify', 'comparison'):
                pass
        record = json.loads(output.getvalue().removeprefix('TIMING: '))
        self.assertEqual(record, {'component':'verify', 'phase':'comparison', 'seconds':2.5, 'status':'passed'})

    def test_failure_is_recorded_without_swallowing_original_error(self):
        output = io.StringIO()
        error = ValueError('original quality failure')
        with contextlib.redirect_stderr(output):
            with self.assertRaises(ValueError) as caught:
                with measure('star', 'validation'):
                    raise error
        self.assertIs(caught.exception, error)
        self.assertEqual(json.loads(output.getvalue().removeprefix('TIMING: '))['status'], 'failed')
