import asyncio
import json
from pathlib import Path
import sys
import time
import aiohttp
import user_pb2 as user

async def main():
    account=json.loads(Path('/bench/private/accounts.json').read_text())[0]
    rows=[]
    async with aiohttp.ClientSession(trust_env=False) as session:
        for i in range(5):
            start=time.perf_counter()
            try:
                async with session.post('http://soarim-bench-gateway:9000/service/user/get_user_info',
                    data=user.GetUserInfoReq(session_id=account['token']).SerializeToString(),
                    timeout=aiohttp.ClientTimeout(total=3)) as raw:
                    result=user.GetUserInfoRsp.FromString(await raw.read())
                rows.append({'success':result.success,'elapsed_ms':(time.perf_counter()-start)*1000})
            except Exception as error:
                rows.append({'success':False,'error':type(error).__name__,'elapsed_ms':(time.perf_counter()-start)*1000})
    result={'context':sys.argv[1],'attempts':len(rows),'success':sum(x['success'] for x in rows),'requests':rows}
    Path('/bench/results/'+sys.argv[1]+'.json').write_text(json.dumps(result,indent=2))
    print(json.dumps(result))

asyncio.run(main())
