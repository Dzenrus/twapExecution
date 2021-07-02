import asyncio
import json
import threading
from datetime import datetime

import websockets

from twapExecution.exchanges.deribit.deribitClient import DeribitClient
from twapExecution.exchanges.env import env_vars

"""
This is a Binance WS Manager, which connects to Binance WS as a client, and starts different streamings;
"""


class DeribitWSManager(threading.Thread):
    def __init__(self, account):
        super().__init__()

        self._account = account.upper()

        self._base_url = env_vars['DERIBIT_URL']
        self._ws_url = env_vars['DERIBIT_WS_URL']

        _api = env_vars[f'DERIBIT_API_KEY_{self._account}']
        _secret = env_vars[f'DERIBIT_SECRET_KEY_{self._account}']

        self.rest_client = DeribitClient(_api,
                                         _secret,
                                         self._ws_url)

        # Only main account can get fee rate;
        self.main_client = DeribitClient(env_vars[f'DERIBIT_API_KEY_MAIN'],
                                         env_vars[f'DERIBIT_SECRET_KEY_MAIN'],
                                         self._ws_url)

        self._conn = []
        self.cur_id = 0

    def _add_user_stream(self, symbol):
        options = {"channels": [f"user.trades.{symbol.upper()}.raw"]}

        self._add_conn({
            "jsonrpc": "2.0",
            "id": 100,
            "method": 'private/subscribe',
            "params": options}
        )

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

        j = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "public/auth",
            "params": {
                "grant_type": "client_credentials",
                "client_id": env_vars[f'DERIBIT_API_KEY_{self._account}'],
                "client_secret": env_vars[f'DERIBIT_SECRET_KEY_{self._account}']
            }
        }

        await self.ws.send(json.dumps(j))
        await self.ws.recv()

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

    # Run the thread!
    def run(self):
        asyncio.run(self.receive_message())

    async def _close_conn(self):
        self.keep_running = False
        for conn in self._conn:
            conn["method"] = 'private/unsubscribe'
            await self.ws.send(json.dumps(conn))

    # Close the connection!
    def close(self):
        asyncio.run(self._close_conn())


if __name__ == '__main__':
    ws = DeribitWSManager('sub2')


    def cb(msg):
        print(msg)


    ws.start_user_stream(symbol='btc-perpetual', callback=cb)
    # ws.close()
