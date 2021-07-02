from twapExecution.exchanges.binance.binanceWSManager import BinanceWSManager
from datetime import datetime

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

    def start(self):
        self._ws = BinanceWSManager(market='usdt-futures')
        self._ws.start_bookticker_stream(symbols=['sfpusdt',
                                                  'btcusdt',
                                                  'xrpusdt',
                                                  'ethusdt',
                                                  'bchusdt',
                                                  'ltcusdt',
                                                  'linkusdt',
                                                  'adausdt'],
                                         callback=self.cb)


if __name__ == '__main__':
    t = Test()
    t.start()
