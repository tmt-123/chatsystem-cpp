import asyncio
import json
from pathlib import Path
import time
import uuid
import aiohttp
import base_pb2 as base
import gateway_pb2 as gateway
import message_pb2 as message
import notify_pb2 as notify
import transmite_pb2 as transmit
from load import call, connect, HTTP

ROOT=Path('/bench')

async def main():
    accounts=json.loads((ROOT/'private/accounts.json').read_text())
    dataset=json.loads((ROOT/'private/dataset.json').read_text())
    incoming,receipts,delays={}, {}, {}
    tests=[]
    async with aiohttp.ClientSession(trust_env=False) as session:
        sockets=[await connect(session,account['token']) for account in accounts[:4]]
        async def read(socket):
            async for raw in socket:
                if raw.type != aiohttp.WSMsgType.BINARY: continue
                event=notify.NotifyMessage.FromString(raw.data)
                if event.notify_type == notify.CHAT_MESSAGE_NOTIFY:
                    info=event.new_message_info.message_info
                    incoming.setdefault(info.client_message_id,[]).append(info.message_id)
                    if delays.get(info.client_message_id): await asyncio.sleep(delays[info.client_message_id])
                    frame=gateway.ClientAuthenticationReq()
                    frame.delivery_ack.message_id=info.message_id
                    await socket.send_bytes(frame.SerializeToString())
                elif event.notify_type == notify.MESSAGE_DELIVERY_NOTIFY:
                    receipt=event.delivery_receipt
                    receipts.setdefault(receipt.client_message_id,[]).append({'id':receipt.message_id,'at':time.perf_counter(),
                        'count':receipt.delivered_target_count})
        readers=[asyncio.create_task(read(socket)) for socket in sockets]
        async def send(pair,key):
            request=transmit.NewMessageReq(session_id=accounts[pair*2]['token'],chat_session_id=dataset[pair]['chat'],client_message_id=key)
            request.message.message_type=base.STRING
            request.message.string_message.content='reliability '+key
            return await call(session,'/service/message_transmit/new_message',request,transmit.NewMessageRsp)
        sequential='retry'+uuid.uuid4().hex
        responses=[await send(0,sequential) for _ in range(10)]
        await asyncio.sleep(.2)
        tests.append({'name':'sequential_retry_10','client_id':sequential,'http_success':len(responses),
            'unique_server_ids':len({x.message_id for x in responses}),'receiver_frames':len(incoming.get(sequential,[])),
            'sender_receipts':len(receipts.get(sequential,[]))})
        concurrent='retry'+uuid.uuid4().hex
        responses=await asyncio.gather(*(send(0,concurrent) for _ in range(8)),return_exceptions=True)
        await asyncio.sleep(.3)
        tests.append({'name':'concurrent_retry_8','client_id':concurrent,
            'http_success':sum(not isinstance(x,Exception) for x in responses),
            'unique_server_ids':len({x.message_id for x in responses if not isinstance(x,Exception)}),
            'receiver_frames':len(incoming.get(concurrent,[])),'sender_receipts':len(receipts.get(concurrent,[]))})
        delayed='delay'+uuid.uuid4().hex
        delays[delayed]=.3
        begin=time.perf_counter()
        response=await send(0,delayed)
        frame=gateway.ClientAuthenticationReq(); frame.delivery_ack.message_id=response.message_id
        await sockets[0].send_bytes(frame.SerializeToString()) # sender cannot acknowledge itself
        await sockets[2].send_bytes(frame.SerializeToString()) # unrelated conversation cannot acknowledge
        await asyncio.sleep(.08)
        premature=bool(receipts.get(delayed))
        await asyncio.sleep(.4)
        tests.append({'name':'recipient_ack_and_forgery','client_id':delayed,'initial_status':int(response.delivery_status),
            'initial_target_count':response.delivered_target_count,'forged_ack_accepted':premature,
            'legitimate_receipts':len(receipts.get(delayed,[])),
            'receipt_ms':(receipts[delayed][0]['at']-begin)*1000 if receipts.get(delayed) else None})
        shared='scope'+uuid.uuid4().hex
        cross=[await send(0,shared),await send(1,shared)]
        await asyncio.sleep(.3)
        tests.append({'name':'same_client_id_different_senders','client_id':shared,'http_success':2,
            'unique_server_ids':len({x.message_id for x in cross}), 'expected_mysql_rows':2})
        await sockets[1].close()
        offline='offline'+uuid.uuid4().hex
        response=await send(0,offline)
        await asyncio.sleep(.5)
        socket=await connect(session,accounts[1]['token']); sockets.append(socket)
        recent=await call(session,'/service/message_storage/get_recent',message.GetRecentMsgReq(
            session_id=accounts[1]['token'],chat_session_id=dataset[0]['chat'],msg_count=50),message.GetRecentMsgRsp)
        tests.append({'name':'offline_history_and_reconnect','client_id':offline,'initial_status':int(response.delivery_status),
            'received_live_receipt':bool(receipts.get(offline)), 'history_contains_message':any(x.message_id==response.message_id for x in recent.msg_list)})
        async with session.post(HTTP+'/service/message_transmit/new_message',data=b'\xff') as raw:
            rejected=transmit.NewMessageRsp.FromString(await raw.read())
        tests.append({'name':'malformed_protobuf','rejected':not rejected.success})
        for socket in sockets: await socket.close()
        for task in readers: task.cancel()
        await asyncio.gather(*readers,return_exceptions=True)
    (ROOT/'results/reliability.json').write_text(json.dumps(tests,indent=2))
    print(json.dumps(tests),flush=True)

asyncio.run(main())
