import json
from pathlib import Path
import requests

expected=json.loads(Path('/bench/private/registration-expected.json').read_text())
session=requests.Session(); session.trust_env=False
raw=session.post('http://soarim-bench-es:9200/user/_mget',json={'ids':[x['uid'] for x in expected]},timeout=10)
raw.raise_for_status()
docs={x['_id']:x for x in raw.json()['docs']}
missing=wrong=0
for row in expected:
    doc=docs[row['uid']]
    if not doc.get('found'): missing+=1
    elif doc['_source'].get('user_id')!=row['uid'] or doc['_source'].get('nickname')!=row['nickname']: wrong+=1
result={'mysql_registration_rows':len(expected),'es_documents_missing':missing,'es_documents_with_mismatched_identity':wrong}
Path('/bench/results/registration-audit.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result))
