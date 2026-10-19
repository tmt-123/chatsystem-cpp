"""ES-only A/B probe. NOT an IM throughput result or implemented batch pipeline."""
import json
from pathlib import Path
import time
import uuid

import requests

session = requests.Session()
session.trust_env = False
host = 'http://soarim-bench-es:9200/'
results = []
for repetition in range(3):
    index = 'test-bulk-research-' + uuid.uuid4().hex
    session.put(host+index, json={'mappings': {'properties': {'content': {'type': 'text'}}}}, timeout=10).raise_for_status()
    try:
        order = ['single', 'bulk32'] if repetition % 2 == 0 else ['bulk32', 'single']
        for mode in order:
            begin = time.perf_counter()
            latencies = []
            if mode == 'single':
                for i in range(128):
                    start = time.perf_counter()
                    response = session.post(host+index+'/_doc/single-'+str(i), json={'content': 'x'*256}, timeout=10)
                    response.raise_for_status()
                    latencies.append((time.perf_counter()-start)*1000)
            else:
                for start_at in range(0, 128, 32):
                    lines = []
                    for i in range(start_at, start_at+32):
                        lines.extend([json.dumps({'index': {'_index': index, '_id': 'bulk32-'+str(i)}}),
                                      json.dumps({'content': 'x'*256})])
                    start = time.perf_counter()
                    response = session.post(host+'_bulk', data=('\n'.join(lines)+'\n').encode(),
                        headers={'Content-Type': 'application/x-ndjson'}, timeout=10)
                    response.raise_for_status()
                    assert not response.json()['errors'], 'bulk contained partial failures'
                    latencies.append((time.perf_counter()-start)*1000)
            elapsed = time.perf_counter()-begin
            results.append({'repetition': repetition+1, 'mode': mode, 'documents': 128, 'requests': len(latencies),
                            'elapsed_seconds': elapsed, 'documents_per_second': 128/elapsed,
                            'request_latencies_ms': latencies})
        response = session.post(host+index+'/_mget', json={'ids': [mode+'-'+str(i) for mode in ('single','bulk32') for i in range(128)]}, timeout=10)
        response.raise_for_status()
        assert sum(doc.get('found', False) for doc in response.json()['docs']) == 256
    finally:
        session.delete(host+index, timeout=10).raise_for_status()
result = {'scope': 'ES only; Python requests client; 256-byte content; no SQL/MQ/WS; not integrated in server',
          'batch_size': 32, 'durability': 'unchanged REQUEST default', 'samples': results}
Path('/bench/results/es-bulk-research.json').write_text(json.dumps(result, indent=2))
print(json.dumps([{k:v for k,v in row.items() if k != 'request_latencies_ms'} for row in results]), flush=True)
