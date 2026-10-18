"""Isolated API regressions. Generated credentials stay in /bench/private only."""
import asyncio
import json
from pathlib import Path
import secrets
import sys
import time
import uuid

import aiohttp
import user_pb2 as user
from load import HTTP, call, connect

ROOT = Path('/bench')


async def registrations(name, count, concurrency):
    prefix = 'opt' + uuid.uuid4().hex[:10]
    semaphore = asyncio.Semaphore(concurrency)
    async with aiohttp.ClientSession(trust_env=False) as session:
        async def one(index):
            async with semaphore:
                nickname = prefix + str(index)
                started = time.perf_counter()
                try:
                    await call(session, '/service/user/username_register',
                        user.UserRegisterReq(nickname=nickname, password=secrets.token_hex(6)), user.UserRegisterRsp)
                    row = {'nickname': nickname, 'success': True}
                except Exception as error:
                    row = {'nickname': nickname, 'success': False, 'error': str(error)}
                row['elapsed_ms'] = (time.perf_counter() - started) * 1000
                return row
        rows = await asyncio.gather(*(one(i) for i in range(count)))
    result = {'prefix': prefix, 'count': count, 'concurrency': concurrency,
              'success': sum(row['success'] for row in rows), 'rows': rows}
    (ROOT/'results'/f'{name}.json').write_text(json.dumps(result, indent=2))
    print(json.dumps({k: v for k, v in result.items() if k != 'rows'}), flush=True)


async def high_fds():
    accounts = json.loads((ROOT/'private/accounts.json').read_text())
    gate = asyncio.Semaphore(20)
    sockets = []
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=1600), trust_env=False) as session:
        async def one(account):
            async with gate:
                socket = await connect(session, account['token'])
                sockets.append(socket)
        await asyncio.gather(*(one(account) for account in accounts[:500]))
        await asyncio.gather(*(socket.close() for socket in sockets))
        sockets.clear()
        await asyncio.gather(*(one(account) for account in accounts[:1000]))
        async def drain(socket):
            async for _ in socket:
                pass
        readers = [asyncio.create_task(drain(socket)) for socket in sockets]
        (ROOT/'results/high-fd-ready.json').write_text(json.dumps({'opened': len(sockets), 'epoch': time.time()}))
        rows = []
        for batch in range(2):
            async def profile(index):
                begin = time.perf_counter()
                try:
                    await call(session, '/service/user/get_user_info',
                        user.GetUserInfoReq(session_id=accounts[index]['token']), user.GetUserInfoRsp)
                    return {'success': True, 'ms': (time.perf_counter()-begin)*1000}
                except Exception as error:
                    return {'success': False, 'error': str(error)}
            for start in range(0, 100, 8):
                rows.extend(await asyncio.gather(*(profile(i) for i in range(start, min(start+8, 100)))))
            if batch == 0:
                await asyncio.sleep(40)
        alive = sum(not socket.closed for socket in sockets)
        await asyncio.gather(*(asyncio.wait_for(socket.close(), 3) for socket in sockets), return_exceptions=True)
        for reader in readers:
            reader.cancel()
        await asyncio.gather(*readers, return_exceptions=True)
    result = {'opened': 1000, 'alive_after_40_seconds': alive, 'http_attempts': len(rows),
              'http_success': sum(row['success'] for row in rows), 'requests': rows}
    (ROOT/'results/high-fd.json').write_text(json.dumps(result, indent=2))
    print(json.dumps({k: v for k, v in result.items() if k != 'requests'}), flush=True)


async def profiles():
    account = json.loads((ROOT/'private/accounts.json').read_text())[0]
    async with aiohttp.ClientSession(trust_env=False) as session:
        async def update(index):
            await call(session, '/service/user/set_description',
                user.SetUserDescriptionReq(session_id=account['token'], description=f'outbox-update-{index}'),
                user.SetUserDescriptionRsp)
        results = await asyncio.gather(*(update(i) for i in range(16)), return_exceptions=True)
        result = {'attempts': len(results), 'success': sum(not isinstance(x, Exception) for x in results),
                  'errors': [str(x) for x in results if isinstance(x, Exception)]}
        (ROOT/'results/profile-updates.json').write_text(json.dumps(result, indent=2))
        print(json.dumps(result))


if __name__ == '__main__':
    if sys.argv[1] == 'register':
        asyncio.run(registrations(sys.argv[2], int(sys.argv[3]), int(sys.argv[4])))
    elif sys.argv[1] == 'high-fd':
        asyncio.run(high_fds())
    elif sys.argv[1] == 'profiles':
        asyncio.run(profiles())
