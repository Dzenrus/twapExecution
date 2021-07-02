import asyncio
import json
import threading
from datetime import datetime

import websockets
import requests
from twapExecution.exchanges.huobi.huobiSpotClient import HuobiSpotClient, UrlParamsBuilder
from twapExecution.exchanges.env import env_vars
from urllib import parse
import hmac
import base64
import hashlib

"""
This is a HUOBI WS Manager, which connects to Binance WS as a client, and starts different streamings;
"""


def generate_huobi_signature_v2(api_key, secret_key, method, url, builder):
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    builder.put_url("accessKey", api_key)
    builder.put_url("signatureVersion", "2.1")
    builder.put_url("signatureMethod", "HmacSHA256")
    builder.put_url("timestamp", timestamp)

    host = parse.urlparse(url).hostname
    path = parse.urlparse(url).path

    # 对参数进行排序:
    keys = sorted(builder.param_map.keys())
    # 加入&
    qs0 = '&'.join(['%s=%s' % (key, parse.quote(builder.param_map[key], safe='')) for key in keys])
    # 请求方法，域名，路径，参数 后加入`\n`
    payload0 = '%s\n%s\n%s\n%s' % (method, host, path, qs0)
    dig = hmac.new(secret_key.encode('utf-8'), msg=payload0.encode('utf-8'), digestmod=hashlib.sha256).digest()
    # 进行base64编码
    s = base64.b64encode(dig).decode()
    builder.put_url("signature", s)
    builder.put_url("authType", "api")

    params = {
        "authType": "api",
        "accessKey": api_key,
        "signatureMethod": "HmacSHA256",
        "signatureVersion": "2.1",
        "timestamp": timestamp,
        "signature": s,
    }

    builder.put_url("action", "req")
    builder.put_url("ch", "auth")
    builder.put_url("params", params)


class HuobiWSManager(threading.Thread):
    def __init__(self, market):
        super().__init__()

        self.market = market.lower()

        if self.market == 'spot':
            self._base_url = env_vars['HUOBI_SPOT_URL']
            self._ws_url = env_vars['HUOBI_SPOT_WS_URL']

        self.rest_client = HuobiSpotClient(env_vars['HUOBI_API_KEY'],
                                           env_vars['HUOBI_SECRET_KEY'],
                                           self._base_url)

        self._conn = []
        self.cur_id = 0

    def _add_user_stream(self, symbol):
        self._add_conn({'action': "sub",
                        'ch': f"orders#{symbol}"})

    def _add_conn(self, sub):
        self._conn.append(sub)
        self.cur_id += 1

    # Start user Stream
    def start_user_stream(self, symbol, callback):
        self._add_user_stream(symbol)
        self._callback = callback
        self.start()

    # Uses callback function to handle messages
    async def receive_message(self):
        self.keep_running = True
        self.ws = await websockets.connect(self._ws_url)

        builder = UrlParamsBuilder()
        generate_huobi_signature_v2(env_vars['HUOBI_API_KEY'],
                                    env_vars['HUOBI_SECRET_KEY'],
                                    'GET',
                                    self._ws_url,
                                    builder)
        await self.ws.send(builder.build_url_to_json())

        for conn in self._conn:
            await self.ws.send(json.dumps(conn))
        while self.keep_running:
            try:
                message = await self.ws.recv()
                message = json.loads(message)

                if message['action'] == 'push':
                    self._callback(message)

                if message['action'] == 'ping':
                    await self.ws.send(json.dumps({'action': 'pong', 'data': {'ts':message['data']['ts']}}))

            except Exception as e:
                if not self.ws.open:
                    print(f'ERROR: Reconnected {e}')
                    self.ws = await websockets.connect(self._ws_url)
                    for conn in self._conn:
                        await self.ws.send(json.dumps(conn))
        else:
            await self.ws.close()

    # Run the thread!
    def run(self):
        asyncio.run(self.receive_message())

    async def _close_conn(self):
        self.keep_running = False
        for conn in self._conn:
            conn['action'] = 'un' + conn['action']
            print(conn)
            await self.ws.send(json.dumps(conn))

    # Close the connection!
    def close(self):
        asyncio.run(self._close_conn())


if __name__ == '__main__':
    huobi = HuobiWSManager('spot')


    def cb(message):
        print(message)


    huobi.start_user_stream(symbol='linausdt', callback=cb)
