import time
from binance.client import Client # Import the Binance Client
from binance import ThreadedWebsocketManager, BinanceSocketManager
from datetime import datetime
# Although fine for tutorial purposes, your API Keys should never be placed directly in the script like below.
# You should use a config file (cfg or yaml) to store them and reference when needed.
PUBLIC = '<YOUR-PUBLIC-KEY>'
SECRET = '<YOUR-SECRET-KEY>'

# Instantiate a Client
client = Client(api_key=PUBLIC, api_secret=SECRET)

# Instantiate a BinanceSocketManager, passing in the client that you instantiated
twm = ThreadedWebsocketManager(api_key=PUBLIC, api_secret=SECRET)
# twm = BinanceSocketManager()


def cb(msg):
    cur_time = datetime.now()
    if 'data' in msg:
        transaction_time = datetime.fromtimestamp(msg['data']['T'] / 1e3)
        print(f'Before precessing Delayed: {"%.3f" % ((cur_time - transaction_time).total_seconds() * 1e3)}ms')


def cb2(msg):
    print(msg)


twm.start()
twm.start_book_ticker_socket(cb)

# twm.start_trade_socket(cb, symbol='btcusdt')
# twm.start_futures_socket(cb2)
#
# for i in range(10):
#     print(i)
#     print(...)
