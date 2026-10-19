"""Analyze microbatch A/B evidence; successful responses are not storage capacity."""
import argparse
import html
import json
from pathlib import Path
import re

from analyze import percentile, resource_stats


def read(path, default=None):
    return json.loads(path.read_text(encoding='utf-8')) if path.exists() else default


def counters(path):
    if not path.exists():
        return {}
    text = path.read_text(encoding='utf-8')
    pairs = re.findall(r'id="value-(message_persist_[a-z_]+)">([^<]+)', text)
    if not pairs:
        pairs = re.findall(r'(message_persist_[a-z_]+)\s*:\s*([^\r\n]+)', text)
    cumulative = {'message_persist_batches', 'message_persist_items', 'message_persist_acks',
                  'message_persist_retries', 'message_persist_rejects', 'message_persist_callback_count'}
    return {key: int(html.unescape(value)) for key, value in pairs if key in cumulative and value.isdigit()}


def analyze(root):
    cases = []
    for path in sorted(root.glob('p2-*/summary.json')):
        folder = path.parent
        row = read(path)
        for filename in ['persistence.json', 'elasticsearch.json', 'final-audit.json']:
            row.update(read(folder/filename, {}))
        row['variant'] = read(folder/'variant.json', {})
        row.update(resource_stats(folder, row['measurement_start_epoch'], row['measurement_end_epoch']))
        samples = [json.loads(line) for line in (folder/'queue.jsonl').read_text().splitlines()] if (folder/'queue.jsonl').exists() else []
        samples = [value for value in samples if row['measurement_start_epoch'] <= value['timestamp'] <= row['measurement_end_epoch']]
        row['queue_ready_sample_peak'] = max((value['ready'] for value in samples), default=None)
        row['queue_ready_samples'] = [value['ready'] for value in samples]
        row['mysql_rate_from_samples'] = ((samples[-1]['mysql_rows']-samples[0]['mysql_rows'])/
                                         (samples[-1]['timestamp']-samples[0]['timestamp'])) if len(samples)>1 else None
        before, after = counters(folder/'brpc-before.txt'), counters(folder/'brpc-after.txt')
        row['batch_counter_deltas_all_phases'] = {key: after[key]-before[key] for key in after if key in before}
        delta = row['batch_counter_deltas_all_phases']
        count = delta.get('message_persist_batches', 0)
        row['mean_callback_batch_size_all_phases'] = delta.get('message_persist_items', 0)/count if count else None
        cases.append(row)
    pooled = {}
    groups = {
        'original100': ['p2-original100', 'p2-original100-r2'],
        'batch32_500': ['p2-v2-batch500-b32', 'p2-v2-batch500-b32-r2', 'p2-v2-batch500-b32-r3'],
    }
    for group, names in groups.items():
        rows = []
        for name in names:
            path = root/name/'messages.jsonl'
            if path.exists(): rows.extend(value for value in (json.loads(line) for line in path.read_text().splitlines()) if value['measurement'])
        good = [row for row in rows if row['http_ok']]
        pooled[group] = dict(attempted=len(rows), http_success=len(good),
                             receipts=sum('receipt_latency_ms' in row for row in good),
                             http_p95_ms=percentile([row['http_latency_ms'] for row in good], .95),
                             http_p99_ms=percentile([row['http_latency_ms'] for row in good], .99),
                             ack_p95_ms=percentile([row['receipt_latency_ms'] for row in good if 'receipt_latency_ms' in row], .95))
    result = dict(cases=cases, pooled=pooled)
    (root/'batch-analysis.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    for row in cases:
        print(json.dumps({key: row.get(key) for key in ['name','offered_rps','attempted','http_success','http_errors',
            'http_ms','receipt_ms','mysql_rate_from_samples','queue_ready_sample_peak','queue_ready_samples',
            'drain_wait_seconds','missing_in_es','missing_accepted_in_mysql','mean_callback_batch_size_all_phases',
            'backend_cpu_mean_percent_one_core_100','backend_memory_working_set_peak_mib']}))
    print(json.dumps(pooled))
    return result


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('root', type=Path)
    analyze(parser.parse_args().root)
