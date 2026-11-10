"""Summarize pipeline timings without reading or changing lakehouse data."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import statistics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path, help='One run directory, not a mixture of machines or campaigns')
    args = parser.parse_args()
    reports = []
    skipped = []
    for path in sorted(args.directory.rglob('report.json')):
        try:
            report = json.loads(path.read_text())
            if not isinstance(report.get('stages'), list):
                skipped.append(str(path))
                continue
            reports.append(report)
        except (ValueError, OSError) as error:
            parser.error(f'{path}: {error}')
    if not reports:
        parser.error('No pipeline reports found')
    groups = defaultdict(list)
    for r in reports:
        groups[(r.get('csv'), r.get('rate'), r.get('max_offsets'))].append(r)
    for (csv, rate, offsets), items in groups.items():
        print(f'\nSource: {csv}\nRate: {rate}; max_offsets: {offsets}; reports: {len(items)}')
        print('Statuses:', dict((status, sum(r.get('status') == status for r in items)) for status in sorted({str(r.get('status')) for r in items})))
        total = sum(float(r.get('seconds', 0)) for r in items)
        print(f'Sum of recorded execution time: {total / 3600:.3f} hours (not wall-clock elapsed time)')
        stages = defaultdict(list)
        for r in items:
            for stage in r['stages']:
                if 'seconds' in stage:
                    stages[stage['name']].append(float(stage['seconds']))
        print('Stage | calls | sum seconds | median seconds | max seconds | share of recorded run time')
        for name, times in sorted(stages.items(), key=lambda kv: -sum(kv[1])):
            share = f'{100*sum(times)/total:.2f}%' if total else 'n/a'
            print(f'{name} | {len(times)} | {sum(times):.2f} | {statistics.median(times):.2f} | {max(times):.2f} | {share}')
        passed = sorted((r for r in items if r.get('status') == 'passed'), key=lambda r: r.get('start_row', 1))
        ranges = [(int(r.get('start_row', 1)), int(r['max_messages'])) for r in passed]
        if ranges:
            print('Passed row intervals:', ranges)
            anomalies = [(previous, current) for previous, current in zip(ranges, ranges[1:]) if current[0] != previous[1] + 1]
            print('Gaps or overlaps between recorded passed batches:', anomalies or 'none')
            print('Intervals describe requested/verified prefixes; retries are not additional unique transactions.')
    if skipped:
        print('Non-pipeline reports excluded:', skipped)
    print('\nThese reports do not measure peak RAM, disk growth or AWS billing. Do not infer these metrics from stage timings.')


if __name__ == '__main__':
    main()
