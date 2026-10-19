"""Compare measured variants without turning burst acceptance into capacity."""
import json
from pathlib import Path
import sys

from analyze import percentile, resource_stats


def analyze(root):
    cases = []
    for path in sorted(root.glob('*/summary.json')):
        folder = path.parent
        data = json.loads(path.read_text())
        for filename in ('persistence.json', 'elasticsearch.json'):
            source = folder/filename
            if source.exists():
                data.update(json.loads(source.read_text()))
        data.update(resource_stats(folder, data['measurement_start_epoch'], data['measurement_end_epoch']))
        samples = [json.loads(line) for line in (folder/'queue.jsonl').read_text().splitlines()] if (folder/'queue.jsonl').exists() else []
        during = [row for row in samples if data['measurement_start_epoch'] <= row['timestamp'] <= data['measurement_end_epoch']]
        data['queue_ready_sample_peak'] = max((row['ready'] for row in during), default=None)
        data['queue_ready_first_sample'] = during[0]['ready'] if during else None
        data['queue_ready_last_sample'] = during[-1]['ready'] if during else None
        if len(during)>1:
            data['mysql_rate_from_samples'] = (during[-1]['mysql_rows']-during[0]['mysql_rows'])/(during[-1]['timestamp']-during[0]['timestamp'])
        else:
            data['mysql_rate_from_samples'] = None
        cases.append(data)
    (root/'optimization-analysis.json').write_text(json.dumps(cases, indent=2))
    pooled_rows = []
    for name in ('optimized-steady50', 'optimized-steady50-r2', 'optimized-steady50-r3'):
        path = root/name/'messages.jsonl'
        if path.exists():
            pooled_rows.extend(row for row in (json.loads(line) for line in path.read_text().splitlines()) if row['measurement'])
    good = [row for row in pooled_rows if row['http_ok']]
    pooled = {'attempted': len(pooled_rows), 'http_success': len(good),
              'receipt_success': sum('receipt_latency_ms' in row for row in good),
              'http_p95_ms': percentile([row['http_latency_ms'] for row in good], .95),
              'http_p99_ms': percentile([row['http_latency_ms'] for row in good], .99),
              'ack_p95_ms': percentile([row['receipt_latency_ms'] for row in good if 'receipt_latency_ms' in row], .95),
              'ack_p99_ms': percentile([row['receipt_latency_ms'] for row in good if 'receipt_latency_ms' in row], .99)}
    (root/'pooled-steady50.json').write_text(json.dumps(pooled, indent=2))
    for case in cases:
        print(json.dumps({key: case.get(key) for key in ('name', 'attempted', 'http_success', 'http_errors', 'http_ms', 'receipt_ms',
            'queue_ready_sample_peak', 'queue_ready_first_sample', 'queue_ready_last_sample', 'mysql_rate_from_samples', 'mysql_rows',
            'missing_accepted_in_mysql', 'missing_in_es', 'drain_wait_seconds', 'backend_cpu_mean_percent_one_core_100',
            'backend_memory_working_set_peak_mib')}))


if __name__ == '__main__':
    analyze(Path(sys.argv[1]))
