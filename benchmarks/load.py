"""Bounded, open-loop IM load driver. All timing uses this process's monotonic clock."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import secrets
import statistics
import sys
import time
import uuid

import aiohttp
import base_pb2 as base
import user_pb2 as user
import gateway_pb2 as gateway
import notify_pb2 as notify
import transmite_pb2 as transmit

ROOT = Path(os.getenv("BENCH_ROOT", "/bench"))
HTTP = "http://soarim-bench-gateway:9000"
WS = "ws://soarim-bench-gateway:9001"

def percentile(values, p):
    if not values: return None
    values = sorted(values)
    return values[min(len(values) - 1, max(0, int((len(values) - 1) * p)))]

async def call(session, path, request, response):
    request.request_id = uuid.uuid4().hex
    async with session.post(HTTP + path, data=request.SerializeToString(), timeout=aiohttp.ClientTimeout(total=8)) as raw:
        raw.raise_for_status()
        result = response.FromString(await raw.read())
        if not result.success: raise RuntimeError(result.errmsg)
        return result

async def connect(session, token):
    socket = await asyncio.wait_for(session.ws_connect(WS, autoping=True),8)
    await socket.send_bytes(gateway.ClientAuthenticationReq(session_id=token).SerializeToString())
    message = await asyncio.wait_for(socket.receive(), 8)
    assert message.type == aiohttp.WSMsgType.BINARY, "no binary auth response"
    result = notify.NotifyMessage.FromString(message.data)
    assert result.notify_type == notify.AUTHENTICATION_NOTIFY and result.authentication.success, "authentication rejected"
    return socket

async def seed(args):
    target = ROOT / "private/accounts.json"
    if target.exists():
        print(json.dumps({"seeded": len(json.loads(target.read_text())), "reused": True})); return
    prefix = uuid.uuid4().hex[:8]
    semaphore = asyncio.Semaphore(1)
    accounts = []
    async with aiohttp.ClientSession(trust_env=False) as session:
        async def one(index):
            async with semaphore:
                nickname, password = f"bench{prefix}{index}", secrets.token_hex(6)
                await call(session, "/service/user/username_register", user.UserRegisterReq(nickname=nickname, password=password), user.UserRegisterRsp)
                login = await call(session, "/service/user/username_login", user.UserLoginReq(nickname=nickname, password=password), user.UserLoginRsp)
                info = await call(session, "/service/user/get_user_info", user.GetUserInfoReq(session_id=login.login_session_id), user.GetUserInfoRsp)
                socket = await connect(session, login.login_session_id)
                await socket.close() # release lease, retain reconnectable token
                return {"uid": info.user_info.user_id, "token": login.login_session_id}
        accounts = await asyncio.gather(*(one(i) for i in range(args.pairs * 2)))
    target.write_text(json.dumps(accounts)); target.chmod(0o600)
    print(json.dumps({"seeded": len(accounts), "pairs": args.pairs}))

async def load(args):
    accounts = json.loads((ROOT / "private/accounts.json").read_text())
    dataset = json.loads((ROOT / "private/dataset.json").read_text())
    run_id = uuid.uuid4().hex[:16]
    records, sockets, readers = {}, [], []
    auth_sem = asyncio.Semaphore(20)
    counters = {"unexpected_notifications": 0, "receiver_frames": 0, "duplicate_receipts": 0, "socket_errors": 0}
    target = ROOT / "results" / args.name
    target.mkdir(parents=True, exist_ok=True)
    # WebSockets retain connector slots; reserve another slot per HTTP sender.
    connector = aiohttp.TCPConnector(limit=max(100, args.concurrency * 3 + 32), ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector, trust_env=False) as session:
        async def open_one(account):
            async with auth_sem:
                socket = await connect(session, account["token"])
                sockets.append(socket)
                return socket
        pairs = await asyncio.gather(*(asyncio.gather(open_one(accounts[2*i]), open_one(accounts[2*i+1])) for i in range(args.concurrency)))
        async def reader(socket):
            try:
                async for raw in socket:
                    if raw.type != aiohttp.WSMsgType.BINARY: continue
                    message = notify.NotifyMessage.FromString(raw.data)
                    if message.notify_type == notify.CHAT_MESSAGE_NOTIFY:
                        info = message.new_message_info.message_info
                        record = records.get(info.client_message_id)
                        if record is not None:
                            record["receiver_seen"] += 1
                            counters["receiver_frames"] += 1
                            record.setdefault("receiver_ms", (time.perf_counter() - start) * 1000)
                        ack = gateway.ClientAuthenticationReq()
                        ack.delivery_ack.message_id = info.message_id
                        await socket.send_bytes(ack.SerializeToString())
                    elif message.notify_type == notify.MESSAGE_DELIVERY_NOTIFY:
                        ack = message.delivery_receipt
                        record = records.get(ack.client_message_id)
                        if record is None:
                            counters["unexpected_notifications"] += 1
                        elif "receipt_ms" in record:
                            counters["duplicate_receipts"] += 1
                        else:
                            record["receipt_ms"] = (time.perf_counter() - start) * 1000
                            record["receipt_latency_ms"] = record["receipt_ms"] - record["start_ms"]
                            record["receipt_count"] = ack.delivered_target_count
            except Exception as error:
                counters["socket_errors"] += 1
        start = time.perf_counter() + 0.25
        run_start_epoch = time.time() + start - time.perf_counter()
        (target/"run.json").write_text(json.dumps({"run_id":run_id,"run_start_epoch":run_start_epoch,
            "measurement_start_epoch":run_start_epoch+args.warmup,"measurement_end_epoch":run_start_epoch+args.warmup+args.seconds}))
        for pair in pairs:
            readers.extend(asyncio.create_task(reader(socket)) for socket in pair)
        duration = args.warmup + args.seconds
        body = ("bench" + run_id + " " + "x" * 256)[:256]
        async def worker(index):
            tick = 0
            while True:
                scheduled = start + index / (args.concurrency * args.frequency) + tick / args.frequency
                if scheduled >= start + duration: break
                await asyncio.sleep(max(0, scheduled - time.perf_counter()))
                now = time.perf_counter()
                if now >= start + duration: break
                client_id = run_id + uuid.uuid4().hex[:16]
                record = {"client_id": client_id, "chat_id": dataset[index]["chat"], "start_ms": (now-start)*1000,
                    "scheduled_ms": (scheduled-start)*1000, "start_lag_ms": max(0, (now-scheduled)*1000),
                    "measurement": now >= start + args.warmup, "receiver_seen": 0, "http_ok": False}
                records[client_id] = record
                req = transmit.NewMessageReq(request_id=uuid.uuid4().hex, session_id=accounts[2*index]["token"],
                    chat_session_id=dataset[index]["chat"], client_message_id=client_id)
                req.message.message_type = base.STRING
                req.message.string_message.content = body
                try:
                    async with session.post(HTTP + "/service/message_transmit/new_message", data=req.SerializeToString(),
                            timeout=aiohttp.ClientTimeout(total=5)) as raw:
                        raw.raise_for_status()
                        response = transmit.NewMessageRsp.FromString(await raw.read())
                    record.update(http_ok=response.success, message_id=response.message_id, http_status=int(response.delivery_status))
                    if not response.success: record["error"] = response.errmsg
                except Exception as error:
                    record["error"] = type(error).__name__ + ": " + str(error)
                record["http_done_ms"] = (time.perf_counter()-start)*1000
                record["http_latency_ms"] = record["http_done_ms"] - record["start_ms"]
                tick += 1
                # Bound overload: never build an unbounded catch-up queue.
                while start + index/(args.concurrency*args.frequency) + tick/args.frequency < time.perf_counter():
                    tick += 1
        await asyncio.gather(*(worker(i) for i in range(args.concurrency)))
        for _ in range(100):
            if all("receipt_ms" in row for row in records.values() if row["http_ok"]): break
            await asyncio.sleep(0.1)
        await asyncio.gather(*(asyncio.wait_for(socket.close(),3) for socket in sockets),return_exceptions=True)
        for task in readers: task.cancel()
        await asyncio.gather(*readers, return_exceptions=True)
    rows = [row for row in records.values() if row["measurement"]]
    good = [row for row in rows if row["http_ok"]]
    http_lat = [row["http_latency_ms"] for row in good]
    ack_lat = [row["receipt_latency_ms"] for row in good if "receipt_latency_ms" in row]
    measurement_end = duration*1000
    result = {"run_id": run_id, "name": args.name, "concurrent_sender_users": args.concurrency,
        "measurement_start_epoch":run_start_epoch+args.warmup,"measurement_end_epoch":run_start_epoch+duration,
        "websocket_connections": args.concurrency*2, "offered_rps": args.concurrency*args.frequency,
        "seconds": args.seconds, "warmup_seconds": args.warmup, "body_bytes": len(body.encode()),
        "attempted": len(rows), "expected_scheduled": args.seconds*args.concurrency*args.frequency,
        "http_success": len(good), "http_errors": len(rows)-len(good),
        "http_success_per_second": sum(row["http_done_ms"] <= measurement_end for row in good)/args.seconds,
        "receipt_success": len(ack_lat), "missing_receipts": len(good)-len(ack_lat),
        "http_ms": {k: percentile(http_lat,p) for k,p in [("p50",.5),("p95",.95),("p99",.99)]},
        "receipt_ms": {k: percentile(ack_lat,p) for k,p in [("p50",.5),("p95",.95),("p99",.99)]},
        "generator_lag_ms_p95": percentile([row["start_lag_ms"] for row in rows],.95),
        "receipt_before_http": sum(row.get("receipt_ms",float("inf")) < row["http_done_ms"] for row in good),
        "all_phases_http_success": sum(row["http_ok"] for row in records.values()),
        "error_examples": [row.get("error", "") for row in rows if not row["http_ok"]][:5], **counters}
    with (target / "messages.jsonl").open("w") as output:
        for row in records.values(): output.write(json.dumps(row) + "\n")
    (target / "summary.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result), flush=True)

async def idle(args):
    accounts = json.loads((ROOT / "private/accounts.json").read_text())
    sockets = []
    sem = asyncio.Semaphore(20)
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=args.connections+32),trust_env=False) as session:
        async def one(account):
            async with sem:
                try: sockets.append(await connect(session, account["token"])); return True
                except Exception: return False
        start = time.perf_counter()
        opened = await asyncio.gather(*(one(x) for x in accounts[:args.connections]))
        establish_seconds=time.perf_counter()-start
        hold_start_epoch=time.time()
        async def drain(socket):
            async for _ in socket: pass
        readers = [asyncio.create_task(drain(socket)) for socket in sockets]
        await asyncio.sleep(args.seconds) # readers keep ping/pong serviced
        alive = sum(not socket.closed for socket in sockets)
        await asyncio.gather(*(asyncio.wait_for(socket.close(),3) for socket in sockets),return_exceptions=True)
        for task in readers: task.cancel()
        await asyncio.gather(*readers, return_exceptions=True)
    result = {"requested": args.connections, "opened": sum(opened), "alive_after_hold": alive,
        "establish_seconds":establish_seconds,"hold_start_epoch":hold_start_epoch,"hold_end_epoch":hold_start_epoch+args.seconds,
        "hold_seconds": args.seconds, "elapsed_seconds": time.perf_counter()-start}
    (ROOT/"results"/f"idle-{args.connections}.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result))

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["seed","load","idle"])
    parser.add_argument("--pairs", type=int, default=500)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--connections", type=int, default=100)
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--frequency", type=float, default=1)
    parser.add_argument("--name", default="pilot")
    args = parser.parse_args()
    await {"seed":seed,"load":load,"idle":idle}[args.mode](args)

if __name__ == "__main__": asyncio.run(main())
