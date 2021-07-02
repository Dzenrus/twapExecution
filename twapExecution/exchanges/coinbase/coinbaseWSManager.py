import asyncio
import base64
import hashlib
import hmac
import json
import os
import threading
import time

import websockets
from twapExecution.exchanges.coinbase.coinbasePublicClient import PublicClient
from twapExecution.exchanges.coinbase.coinbaseAuthClient import AuthenticatedClient
from twapExecution.exchanges.env import env_vars


class CoinbaseWSManager(threading.Thread):
    def __init__(self):
        super().__init__()

        self._base_url = env_vars['COINBASE_SPOT_URL']
        self._ws_url = env_vars['COINBASE_WS_URL']

        self.rest_client = AuthenticatedClient(env_vars['COINBASE_API_KEY'],
                                               env_vars['COINBASE_SECRET_KEY'],
                                               env_vars['COINBASE_PW'],
                                               self._base_url)
        self.public_client = PublicClient()
        self._conn = []
        self.cur_id = 0

        timestamp = str(time.time())
        msg = timestamp + 'GET' + '/users/self/verify'
        self._auth_headers = self._get_auth_headers(timestamp,
                                                    msg,
                                                    env_vars['COINBASE_API_KEY'],
                                                    env_vars['COINBASE_SECRET_KEY'],
                                                    env_vars['COINBASE_PW'])

    def _get_auth_headers(self, timestamp, message, api_key, secret_key, passphrase):
        message = message.encode('ascii')
        hmac_key = base64.b64decode(secret_key)
        signature = hmac.new(hmac_key, message, hashlib.sha256)
        signature_b64 = base64.b64encode(signature.digest()).decode('utf-8')
        return {
            'Content-Type': 'Application/JSON',
            'CB-ACCESS-SIGN': signature_b64,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': api_key,
            'CB-ACCESS-PASSPHRASE': passphrase
        }

    def _add_user_stream(self, symbol):
        sub = dict()

        sub['type'] = 'subscribe'
        sub['product_ids'] = symbol
        sub['channels'] = ['user']

        sub['signature'] = self._auth_headers['CB-ACCESS-SIGN']
        sub['key'] = self._auth_headers['CB-ACCESS-KEY']
        sub['passphrase'] = self._auth_headers['CB-ACCESS-PASSPHRASE']
        sub['timestamp'] = self._auth_headers['CB-ACCESS-TIMESTAMP']

        self._add_conn(sub)

    def _add_conn(self, sub):
        self._conn.append(sub)
        self.cur_id += 1

    def start_user_stream(self, symbol, callback):
        self._add_user_stream(symbol)
        self._callback = callback
        self.start()

    async def receive_message(self):
        self.keep_running = True
        self.ws = await websockets.connect(self._ws_url)

        for conn in self._conn:
            await self.ws.send(json.dumps(conn))

        while self.keep_running:
            try:
                message = await self.ws.recv()
                message = json.loads(message)
                if 'id' not in message:
                    self._callback(message)

            except Exception as e:

                if not self.ws.open:
                    print(f'ERROR: Reconnected {e}')
                    self.ws = await websockets.connect(self._ws_url)
                    for conn in self._conn:
                        await self.ws.send(json.dumps(conn))
        else:
            await self.ws.close()

    def run(self):
        asyncio.run(self.receive_message())

    async def _close_conn(self):
        self.keep_running = False
        for conn in self._conn:
            conn['type'] = 'un' + conn['type']
            print(conn)
            await self.ws.send(json.dumps(conn))

    def close(self):
        asyncio.run(self._close_conn())
