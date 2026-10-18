import json
from pathlib import Path
import sys
import requests
folder=Path("/bench/results")/sys.argv[1]
expected=set()
for line in (folder/"messages.jsonl").read_text().splitlines():
    row=json.loads(line)
    if row["http_ok"]: expected.add(row["message_id"])
session=requests.Session(); session.trust_env=False
found=set()
ids=list(expected)
for i in range(0,len(ids),500):
    response=session.post("http://soarim-bench-es:9200/message/_mget",json={"ids":ids[i:i+500]},timeout=10)
    response.raise_for_status()
    found.update(x["_id"] for x in response.json()["docs"] if x.get("found"))
result={"expected_unique_messages":len(expected),"found_in_es":len(found),"missing_in_es":len(expected-found)}
(folder/"elasticsearch.json").write_text(json.dumps(result,indent=2))
print(json.dumps(result))
