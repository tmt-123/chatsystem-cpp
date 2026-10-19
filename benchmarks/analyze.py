"""Aggregate raw benchmark output without treating burst results as sustained capacity."""
import argparse
import csv
import json
from pathlib import Path
import re
import statistics

def read(path, fallback=None):
    return json.loads(path.read_text(encoding='utf-8')) if path.exists() else fallback

def percentile(values,p):
    values=sorted(values)
    return values[min(len(values)-1,int((len(values)-1)*p))] if values else None

def memory_mib(value):
    match=re.match(r'([\d.]+)\s*([KMGT]?i?B)',value.split('/')[0].strip())
    if not match: return 0
    factors={'B':1,'kB':1000,'KB':1000,'MB':1000**2,'GB':1000**3,'TB':1000**4,
        'KiB':1024,'MiB':1024**2,'GiB':1024**3,'TiB':1024**4}
    return float(match[1])*factors[match[2]]/(1024**2)

def resource_stats(folder,start,end):
    groups={}
    for line in (folder/'resources.jsonl').read_text().splitlines() if (folder/'resources.jsonl').exists() else []:
        row=json.loads(line)
        if start<=row['timestamp']<=end:
            groups.setdefault(row['timestamp'],[]).append(row)
    totals=[]; clients=[]
    for rows in groups.values():
        backend=[row for row in rows if row['Name'].startswith(('soarim-bench-', 'soarim-opt-'))
                 and row['Name'] != 'soarim-opt-loadgen']
        totals.append((sum(float(x['CPUPerc'].rstrip('%')) for x in backend),sum(memory_mib(x['MemUsage']) for x in backend)))
        clients.extend(float(x['CPUPerc'].rstrip('%')) for x in rows if x['Name'] in ('soarim-benchmark-loadgen', 'soarim-opt-loadgen'))
    return {'resource_samples':len(totals),'backend_cpu_mean_percent_one_core_100':statistics.mean(x[0] for x in totals) if totals else None,
        'backend_cpu_peak_percent_one_core_100':max((x[0] for x in totals),default=None),
        'backend_memory_working_set_peak_mib':max((x[1] for x in totals),default=None),
        'generator_cpu_mean_percent_one_core_100':statistics.mean(clients) if clients else None,
        'generator_cpu_peak_percent_one_core_100':max(clients,default=None)}

def main():
    parser=argparse.ArgumentParser(); parser.add_argument('root',type=Path); args=parser.parse_args()
    root=args.root
    cases=[]; pooled={}
    for folder in sorted(root.iterdir()):
        if not folder.is_dir() or not re.fullmatch(r'(pilot|c50-r[12]|steady10-r[123]|burst(?:50|100|200|500))',folder.name): continue
        summary=read(folder/'summary.json')
        if not summary: continue
        row={**summary,**(read(folder/'persistence.json',{})),**(read(folder/'elasticsearch.json',{}))}
        row.update(resource_stats(folder,row['measurement_start_epoch'],row['measurement_end_epoch']))
        snapshots=[json.loads(line) for line in (folder/'queue.jsonl').read_text().splitlines()] if (folder/'queue.jsonl').exists() else []
        row['queue_ready_sample_peak']=max((x['ready'] for x in snapshots),default=None)
        samples=[x for x in snapshots if row['measurement_start_epoch']<=x['timestamp']<=row['measurement_end_epoch'] and 'mysql_rows' in x]
        row['mysql_rate_from_samples']=((samples[-1]['mysql_rows']-samples[0]['mysql_rows'])/(samples[-1]['timestamp']-samples[0]['timestamp'])) if len(samples)>1 else None
        cases.append(row)
        group='steady10' if folder.name.startswith('steady10') else 'sustained50' if folder.name.startswith('c50') else folder.name
        pooled.setdefault(group,[]).extend(json.loads(line) for line in (folder/'messages.jsonl').read_text().splitlines() if json.loads(line)['measurement'])
    combined={}
    for group,rows in pooled.items():
        successful=[x for x in rows if x['http_ok']]
        combined[group]={'attempted':len(rows),'http_success':len(successful),'http_errors':len(rows)-len(successful),
            'receipts':sum('receipt_latency_ms' in row for row in successful),
            'http_p95_ms':percentile([x['http_latency_ms'] for x in successful],.95),
            'http_p99_ms':percentile([x['http_latency_ms'] for x in successful],.99),
            'receipt_p95_ms':percentile([x['receipt_latency_ms'] for x in successful if 'receipt_latency_ms' in x],.95),
            'receipt_p99_ms':percentile([x['receipt_latency_ms'] for x in successful if 'receipt_latency_ms' in x],.99)}
    idle=[]
    for path in sorted(root.glob('idle-*.json')):
        value=read(path); folder=root/path.stem
        value.update(resource_stats(folder,value['hold_start_epoch'],value['hold_end_epoch']))
        idle.append(value)
    registration=read(root/'registration.json',[])
    registrations=[]
    for concurrency in sorted({x['concurrency'] for x in registration}):
        subset=[x for x in registration if x['concurrency']==concurrency]
        registrations.append({'concurrency':concurrency,'attempts':sum(x['attempts'] for x in subset),
            'success':sum(x['success'] for x in subset),'errors':sum(x['attempts']-x['success'] for x in subset)})
    result={'cases':cases,'pooled':combined,'idle':idle,'registration':registrations,
        'registration_audit':read(root/'registration-audit.json'),
        'discovery':[read(x) for x in sorted(root.glob('discovery*.json'))],
        'reliability':read(root/'reliability.json'),'faults':read(root/'faults.json'),
        'history_recovery':read(root/'history-recovery.json'),'environment':read(root/'environment.json'),
        'notes':['Latency percentiles use successful requests; errors and missing receipts are listed separately.',
            'CPU percent 100 means one logical core; all backends share CPUs 0-3.',
            'Burst tests last five seconds and must not be advertised as sustained throughput.',
            'Memory is Docker stats working-set accounting, not application heap/RSS.',
            'Queue peak is sampled, not a high-frequency exact maximum.',
            'Post-load verification wait is not per-message persistence latency.']}
    (root/'analysis.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    columns=['name','concurrent_sender_users','websocket_connections','offered_rps','seconds','attempted','http_success','http_errors',
        'receipt_success','missing_receipts','http_success_per_second','queue_ready_sample_peak',
        'backend_cpu_mean_percent_one_core_100','backend_cpu_peak_percent_one_core_100','backend_memory_working_set_peak_mib',
        'generator_lag_ms_p95','mysql_rows','missing_accepted_in_mysql','missing_in_es']
    with (root/'performance.csv').open('w',newline='',encoding='utf-8-sig') as stream:
        writer=csv.DictWriter(stream,fieldnames=columns+['http_p95_ms','http_p99_ms','ack_p95_ms','ack_p99_ms'],extrasaction='ignore')
        writer.writeheader()
        for row in cases:
            writer.writerow({**row,'http_p95_ms':row['http_ms']['p95'],'http_p99_ms':row['http_ms']['p99'],
                'ack_p95_ms':row['receipt_ms']['p95'],'ack_p99_ms':row['receipt_ms']['p99']})
    print(json.dumps({'cases':len(cases),'pooled':combined,'registration':registrations,'idle':idle},ensure_ascii=False,indent=2))

if __name__=='__main__': main()
