"""Real HTTP/WebSocket gateway regression; the brpc backend is a test fixture.

Requires protobuf, requests, websocket-client and a reachable Redis.
Only exact, uniquely named fixture keys in Redis DB 15 are removed at teardown.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import uuid

import requests
import websocket


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", required=True)
    parser.add_argument("--proto-dir", required=True)
    parser.add_argument("--redis-host", default=os.getenv("TEST_REDIS_HOST", "127.0.0.1"))
    args = parser.parse_args()
    sys.path.insert(0, args.proto_dir)
    import base_pb2 as base
    import gateway_pb2 as gateway
    import notify_pb2 as notify
    import transmite_pb2 as transmit

    checks, sockets, metadata = [], [], None
    http = requests.Session()
    http.trust_env = False

    def check(condition, description):
        assert condition, description
        checks.append(description)

    def notification(ws):
        data = ws.recv()
        assert isinstance(data, bytes), "expected binary protobuf notification"
        result = notify.NotifyMessage()
        result.ParseFromString(data)
        return result

    def connect(token):
        ws = websocket.create_connection("ws://127.0.0.1:19001", timeout=3, http_no_proxy=["127.0.0.1"])
        sockets.append(ws)
        ws.send_binary(gateway.ClientAuthenticationReq(session_id=token).SerializeToString())
        result = notification(ws)
        assert result.notify_type == notify.AUTHENTICATION_NOTIFY and result.authentication.success
        return ws

    def ack(ws, message_id):
        frame = gateway.ClientAuthenticationReq()
        frame.delivery_ack.message_id = message_id
        ws.send_binary(frame.SerializeToString())

    def quiet(ws):
        ws.settimeout(0.2)
        try:
            data = ws.recv()
        except websocket.WebSocketTimeoutException:
            return True
        finally:
            ws.settimeout(3)
        return not data

    def send(chat="single", client_id=None):
        request = transmit.NewMessageReq(request_id=uuid.uuid4().hex,
            session_id=metadata["sessions"]["a"], user_id="forged-sender", chat_session_id=chat,
            client_message_id=client_id or uuid.uuid4().hex)
        request.message.message_type = base.STRING
        request.message.string_message.content = "receipt regression"
        response = http.post("http://127.0.0.1:19000/service/message_transmit/new_message",
            data=request.SerializeToString(), timeout=5)
        response.raise_for_status()
        result = transmit.NewMessageRsp()
        result.ParseFromString(response.content)
        assert result.success
        assert result.delivery_status == base.SENT and result.delivered_target_count == 0
        return result

    with tempfile.TemporaryDirectory(prefix="gateway-receipts-") as directory:
        ready = Path(directory) / "fixture.json"
        log = open(Path(directory) / "fixture.log", "wb")
        process = subprocess.Popen([args.fixture, args.redis_host, str(ready)], stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 15
            while not ready.exists():
                if process.poll() is not None: raise RuntimeError("gateway fixture did not start")
                if time.monotonic() > deadline: raise TimeoutError("fixture startup timed out")
                time.sleep(0.05)
            metadata = json.loads(ready.read_text())
            a, b, c = (connect(metadata["sessions"][key]) for key in "abc")
            result = send()
            message = notification(b).new_message_info.message_info
            check(message.sender.user_id == metadata["prefix"] + "-a", "HTTP identity comes from session, not claimed user_id")
            check(quiet(a), "socket enqueue does not produce a delivery receipt")
            ack(c, result.message_id)
            ack(a, result.message_id)
            ack(b, "unknown-message")
            check(quiet(a), "non-recipient, sender and unknown-message ACKs are ignored")
            ack(b, result.message_id)
            receipt = notification(a)
            check(receipt.notify_type == notify.MESSAGE_DELIVERY_NOTIFY
                and receipt.delivery_receipt.message_id == result.message_id
                and receipt.delivery_receipt.client_message_id == result.client_message_id
                and receipt.delivery_receipt.delivered_target_count == 1, "recipient ACK notifies sender with correlated IDs")
            ack(b, result.message_id)
            check(quiet(a), "duplicate ACK does not count or notify twice")

            result = send("group")
            notification(b); notification(c)
            ack(c, result.message_id)
            check(notification(a).delivery_receipt.delivered_target_count == 1, "first group recipient counts once")
            ack(b, result.message_id)
            check(notification(a).delivery_receipt.delivered_target_count == 2, "second group recipient advances unique count")

            b.close()
            time.sleep(0.05)
            result = send()
            check(quiet(a), "offline recipient remains SENT")
            b = connect(metadata["sessions"]["b"])
            check(True, "disconnected client can reauthenticate with the same unexpired token")
            replacement = connect(metadata["sessions"]["b"])
            time.sleep(0.05)
            result = send()
            check(notification(replacement).new_message_info.message_info.message_id == result.message_id,
                  "closing superseded socket preserves replacement connection")
            ack(replacement, result.message_id)
            check(notification(a).delivery_receipt.delivered_target_count == 1, "replacement connection can acknowledge")

            invalid = websocket.create_connection("ws://127.0.0.1:19001", timeout=3)
            sockets.append(invalid)
            invalid.send_binary(gateway.ClientAuthenticationReq(session_id="invalid").SerializeToString())
            check(not notification(invalid).authentication.success, "invalid session receives explicit authentication failure")
            unauthenticated = websocket.create_connection("ws://127.0.0.1:19001", timeout=3)
            sockets.append(unauthenticated)
            ack(unauthenticated, result.message_id)
            check(not notification(unauthenticated).authentication.success, "ACK before authentication is rejected")
            malformed = websocket.create_connection("ws://127.0.0.1:19001", timeout=3)
            sockets.append(malformed)
            malformed.send_binary(b"\xff")
            check(malformed.recv() == "", "malformed protobuf closes only the offending connection")
            result = send()
            check(notification(replacement).new_message_info.message_info.message_id == result.message_id,
                  "gateway remains usable after invalid clients")
        finally:
            for ws in sockets:
                try: ws.close(timeout=0.1)
                except Exception: pass
            process.terminate()
            try: process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait()
            log.close()
            if metadata:
                subprocess.run([args.fixture, args.redis_host, metadata["prefix"], "--cleanup"], check=True)
    print(json.dumps({"passed": len(checks), "checks": checks}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
