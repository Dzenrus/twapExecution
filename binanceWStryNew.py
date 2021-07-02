import websockets
from datetime import datetime
import json
import asyncio

class Test:
    def __init__(self):
        self._n = 0
        self._cur_avg = None

    def cb(self, msg):
        cur_time = datetime.now()
        transaction_time = datetime.fromtimestamp(msg['T'] / 1e3)
        d = (cur_time - transaction_time).total_seconds()*1e3
        if self._n == 0:
            self._cur_avg = d
        else:
            self._cur_avg = (self._cur_avg*self._n + d) / (self._n + 1)
        self._n += 1

        print(self._cur_avg)

    async def async_processing(self):
        self.ws = await websockets.connect('wss://fstream.binance.com/ws')

        d = {
            "method": "SUBSCRIBE",
            "params":
                [
                    "sfpusdt@bookTicker",
                    "btcusdt@bookTicker",
                    "xrpusdt@bookTicker",
                    "ethusdt@bookTicker",
                    "bchusdt@bookTicker",
                    "ltcusdt@bookTicker",
                    "linkusdt@bookTicker",
                    "adausdt@bookTicker",
                ],
            "id": 1
        }
        await self.ws.send(json.dumps(d))

        while True:
            try:
                message = await self.ws.recv()
                message = json.loads(message)
                if 'id' not in message:
                    self.cb(message)

            except websockets.exceptions.ConnectionClosed:
                print('ConnectionClosed')
                is_alive = False
                break

    def start(self):
        asyncio.run(self.async_processing())


if __name__ == '__main__':
    t = Test()
    t.start()