import json
from pathlib import Path
import time
import uuid
import requests
import user_pb2 as user

accounts=json.loads(Path('/bench/private/accounts.json').read_text())
dataset=json.loads(Path('/bench/private/dataset.json').read_text())
session=requests.Session(); session.trust_env=False
results=[]
requests_=[('user_rpc','http://soarim-bench-user:10003/bite_im.UserService/GetUserInfo',
    {'request_id':'diagnostic','user_id':accounts[0]['uid']}),
    ('gateway_profile','http://soarim-bench-gateway:9000/service/user/get_user_info',
     user.GetUserInfoReq(request_id='diagnostic',session_id=accounts[0]['token']).SerializeToString()),
    ('transmit_rpc','http://soarim-bench-transmite:10004/bite_im.MsgTransmitService/GetTransmitTarget',
     {'request_id':'diagnostic','user_id':accounts[0]['uid'],'chat_session_id':dataset[0]['chat'],
      'client_message_id':'diag'+uuid.uuid4().hex,'message':{'message_type':'STRING','string_message':{'content':'diagnostic'}}})]
for name,url,body in requests_:
    start=time.monotonic()
    try:
        raw=session.post(url,json=body if isinstance(body,dict) else None,
            data=body if isinstance(body,bytes) else None,timeout=3)
        results.append({'name':name,'http_status':raw.status_code,'elapsed_ms':(time.monotonic()-start)*1000})
    except Exception as error: results.append({'name':name,'error':type(error).__name__,'elapsed_ms':(time.monotonic()-start)*1000})
Path('/bench/results/after-restart-diagnostics.json').write_text(json.dumps(results,indent=2))
print(json.dumps(results))
