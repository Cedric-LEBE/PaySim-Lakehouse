"""Structured wall-clock timings; spans include any lazy Spark actions they execute."""
from contextlib import contextmanager
import json
import sys
import time


@contextmanager
def measure(component, phase):
    start = time.monotonic()
    status = 'failed'
    try:
        yield
        status = 'passed'
    finally:
        print('TIMING: ' + json.dumps({'component': component, 'phase': phase,
              'seconds': round(time.monotonic() - start, 6), 'status': status}),
              file=sys.stderr, flush=True)
