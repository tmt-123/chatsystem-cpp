import asyncio
import json
from pathlib import Path
import aiohttp
import message_pb2 as message
from load import call

async def main():
    token=json.loads(Path("/bench/private/accounts.json").read_text())[0]["token"]
    chat=json.loads(Path("/bench/private/dataset.json").read_text())[0]["chat"]
    async with aiohttp.ClientSession(trust_env=False) as session:
        try:
            response=await call(session,"/service/message_storage/get_recent",
                message.GetRecentMsgReq(session_id=token,chat_session_id=chat,msg_count=5),message.GetRecentMsgRsp)
            result={"history_works_after_unpause_without_restart":True,"messages":len(response.msg_list)}
        except Exception as error:
            result={"history_works_after_unpause_without_restart":False,"error":str(error)}
    Path("/bench/results/history-recovery.json").write_text(json.dumps(result,indent=2))
    print(json.dumps(result))

asyncio.run(main())
