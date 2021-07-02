import asyncio
import json
import threading
from datetime import datetime

import websockets
from datetime import datetime
from twapExecution.exchanges.okex.okexSpotClient import OkexSpotClient
from twapExecution.exchanges.okex.okexFuturesClient import OkexFuturesClient
from twapExecution.exchanges.env import env_vars
import requests
import dateutil.parser as dp
import hmac
import base64
import zlib

"""
This is a OKEX WS Manager, which connects to OKEX WS as a client, and starts different streamings;
"""


def get_timestamp():
    now = datetime.now()
    t = now.isoformat("T", "milliseconds")
    return t + "Z"


def get_server_time():
    url = "https://www.okex.com/api/general/v3/time"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['iso']
    else:
        return ""


def server_timestamp():
    server_time = get_server_time()
    parsed_t = dp.parse(server_time)
    timestamp = parsed_t.timestamp()
    return timestamp


def login_params(timestamp, api_key, secret_key, passphrase):
    message = timestamp + 'GET' + '/users/self/verify'

    mac = hmac.new(bytes(secret_key, encoding='utf-8'), bytes(message, encoding='utf-8'), digestmod='sha256')
    d = mac.digest()
    sign = base64.b64encode(d)

    login_param = {"op": "login", "args": [api_key, passphrase, timestamp, sign.decode("utf-8")]}
    login_str = json.dumps(login_param)

    return login_str


def inflate(data):
    decompress = zlib.decompressobj(
        -zlib.MAX_WBITS  # see above
    )
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated


class OkexWSManager(threading.Thread):
    def __init__(self, market):
        super().__init__()

        self._base_url = env_vars['OKEX_URL']
        self._ws_url = env_vars['OKEX_WS_URL']

        if 'SPOT' in market.upper():
            self._market = 'spot'
            self.rest_client = OkexSpotClient(env_vars['OKEX_API_KEY'],
                                              env_vars['OKEX_SECRET_KEY'],
                                              env_vars['OKEX_PASSPHRASE'],
                                              self._base_url)

        elif 'FUTURES' in market.upper():
            self._market = 'futures'
            self.rest_client = OkexFuturesClient(env_vars['OKEX_API_KEY'],
                                                 env_vars['OKEX_SECRET_KEY'],
                                                 env_vars['OKEX_PASSPHRASE'],
                                                 self._base_url)

        self._conn = []
        self.cur_id = 0

    def _add_user_stream(self, symbol):
        self._add_conn({'op': "subscribe",
                        'args': [f'{self._market}/order:{symbol.upper()}']})

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

        login_str = login_params(str(server_timestamp()),
                                 env_vars['OKEX_API_KEY'],
                                 env_vars['OKEX_SECRET_KEY'],
                                 env_vars['OKEX_PASSPHRASE'])
        await self.ws.send(login_str)
        msg = await self.ws.recv()
        msg = inflate(msg).decode('utf-8')
        time = get_timestamp()
        print(time + msg)

        for conn in self._conn:
            await self.ws.send(json.dumps(conn))

        while self.keep_running:
            try:
                message = await self.ws.recv()
                message = json.loads(inflate(message).decode('utf-8'))

                if 'event' not in message:
                    self._callback(message)

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
            conn['op'] = 'un' + conn['op']
            print(conn)
            await self.ws.send(json.dumps(conn))

    # Close the connection!
    def close(self):
        asyncio.run(self._close_conn())
