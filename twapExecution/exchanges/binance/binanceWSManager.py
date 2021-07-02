import asyncio
import json
import threading
from datetime import datetime

import websockets

from twapExecution.exchanges.binance.binanceClient import BinanceClient
from twapExecution.exchanges.env import env_vars

"""
This is a Binance WS Manager, which connects to Binance WS as a client, and starts different streamings;
"""


class BinanceWSManager(threading.Thread):
    def __init__(self, market):
        super().__init__()

        self._loop = asyncio.get_event_loop()
        self.market = market.lower()

        if self.market == 'spot':
            self._base_url = env_vars['BINANCE_SPOT_URL']
            self._ws_url = env_vars['BINANCE_SPOT_WS_URL']
        elif 'futures' in self.market:
            if 'usdt' in self.market:
                self._base_url = env_vars['BINANCE_USDT_FUTURES_URL']
                self._ws_url = env_vars['BINANCE_USDT_FUTURES_WS_URL']
            elif 'coin' in self.market:
                self._base_url = env_vars['BINANCE_COIN_FUTURES_URL']
                self._ws_url = env_vars['BINANCE_COIN_FUTURES_WS_URL']

        self.rest_client = BinanceClient(env_vars['BINANCE_API_KEY'],
                                         env_vars['BINANCE_SECRET_KEY'],
                                         self._base_url)
        print(self._base_url, self._ws_url)

        self._conn = []
        self.cur_id = 0

    def _add_aggTrade_stream(self, symbol):
        self._add_conn({'method': 'SUBSCRIBE',
                        'params': [f'{symbol}@aggTrade'],
                        'id': self.cur_id})

    def _add_miniticker_stream(self, symbol):
        self._add_conn({'method': 'SUBSCRIBE',
                        'params': [f'{symbol}@ticker'],
                        'id': self.cur_id})

    def _add_bookticker_stream(self, symbols):
        params = []

        for symbol in symbols:
            params.append(f'{symbol}@bookTicker')

        self._add_conn({'method': "SUBSCRIBE",
                        'params': params,
                        'id': self.cur_id})

    def _add_user_stream(self):
        self._add_conn({'method': "SUBSCRIBE",
                        'params': [self.rest_client.post_user_listen_key()['listenKey']],
                        'id': self.cur_id})

    def _add_candle_stream(self, symbols, interval):
        params = []

        for symbol in symbols:
            params.append(f'{symbol}@kline_{interval}')

        self._add_conn({'method': "SUBSCRIBE",
                        'params': params,
                        'id': self.cur_id})

    def _add_partial_book_depth_stream(self, symbols, level):
        params = []

        for symbol in symbols:
            params.append(f'{symbol}@depth{level}')

        self._add_conn({'method': "SUBSCRIBE",
                        'params': params,
                        'id': self.cur_id})

    def _add_conn(self, sub):
        self._conn.append(sub)
        self.cur_id += 1

    # Start aggTrade Stream
    def start_aggTrade_stream(self, symbols, callback):
        for symbol in symbols:
            self._add_aggTrade_stream(symbol)
        self._callback = callback
        self.start()

    # Start miniticker Stream
    def start_miniticker_stream(self, symbols, callback):
        for symbol in symbols:
            self._add_miniticker_stream(symbol)
        self._callback = callback
        self.start()

    # Start user Stream
    def start_user_stream(self, callback):
        self._add_user_stream()
        self._callback = callback
        self.start()

    def start_bookticker_stream(self, symbols, callback):
        self._add_bookticker_stream(symbols)
        self._callback = callback
        self.start()

    def start_candle_stream(self, symbols, interval, callback):
        self._add_candle_stream(symbols, interval)
        self._callback = callback
        self.start()

    def start_partial_book_depth_stream(self, symbols, level, callback):
        self._add_partial_book_depth_stream(symbols, level)
        self._callback = callback
        self.start()

    # Uses callback function to handle messages
    async def receive_message(self):
        self.keep_running = True
        self.ws = await websockets.connect(self._ws_url)

        for conn in self._conn:
            await self.ws.send(json.dumps(conn))
        while self.keep_running:
            try:
                message = await self.ws.recv()
                message = json.loads(message)
                # print(message)
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

    # Put listen key every N minutes to prevent disconnection
    async def put_listen_key_n_minutes(self, n_minutes=30):
        while self.keep_running:
            print(f'{datetime.now()} PUT LISTEN KEY EVERY {n_minutes} MINUTE(S)')
            self.rest_client.put_user_listen_key()
            await asyncio.sleep(n_minutes * 60)

    # Run the thread!
    def run(self):
        async def _create_jobs():
            jobs = [self.receive_message(), self.put_listen_key_n_minutes()]
            return await asyncio.gather(*jobs)

        asyncio.run(_create_jobs())

    async def _close_conn(self):
        self.keep_running = False
        for conn in self._conn:
            conn['method'] = 'UN' + conn['method']
            print(conn)
            await self.ws.send(json.dumps(conn))

    # Close the connection!
    def close(self):
      asyncio.run(self._close_conn())
# if __name__ == '__main__':
#     from datetime import datetime
#
#     ws = BinanceWSManager(market='usdt-futures')
#
#     def cb(msg):
#         print(msg['s'], datetime.fromtimestamp(msg['E']/1e3))
#         if msg['k']['x']:
#             print(msg)
#
#     coins = ['BTC', 'ETH', 'BNB', 'ADA', 'XRP', 'LTC', 'LINK', 'TRX', 'BCH', 'THETA',
#              'XMR', 'ZEC', 'IOTA', 'ATOM', 'EOS', 'XTZ', 'NEO', 'BAT', 'DASH',
#              'ETC', 'XLM', 'ZRX', 'DOT']
#     ws.start_candle_stream([coin.lower() + 'usdt' for coin in coins],'1m', callback=cb)
