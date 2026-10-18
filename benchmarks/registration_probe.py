import asyncio
import json
import uuid
from pathlib import Path
import aiohttp
import user_pb2 as user
from load import call

async def main():
    results = []
    for concurrency in (1,8):
        for repetition in range(1,4):
            semaphore = asyncio.Semaphore(concurrency)
            async with aiohttp.ClientSession(trust_env=False) as session:
                async def one(i):
                    async with semaphore:
                        try:
                            await call(session,"/service/user/username_register",user.UserRegisterReq(
                                nickname="reg"+uuid.uuid4().hex[:15],password=uuid.uuid4().hex[:12]),user.UserRegisterRsp)
                            return {"ok":True}
                        except Exception as error: return {"ok":False,"error":str(error)}
                rows = await asyncio.gather(*(one(i) for i in range(24)))
            results.append({"concurrency":concurrency,"repetition":repetition,"attempts":len(rows),
                "success":sum(row["ok"] for row in rows),"failures":[row for row in rows if not row["ok"]]})
    Path("/bench/results/registration.json").write_text(json.dumps(results,indent=2))
    print(json.dumps(results),flush=True)

asyncio.run(main())
