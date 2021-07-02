import sqlite3

from twapExecution.exchanges.binance.binanceWSManager import BinanceWSManager
from twapExecution.exchanges.env import env_vars
from twapExecution.tgBot.tgBotAPI import TgBotAPI


class BinanceSimpleTWAP:
    def __init__(self, exchange, market, coin, qty, side, execution_minutes, execution_freq_per_minute, cont):
        print(exchange)

        self.binance = BinanceWSManager(market)

        self.chat_id = env_vars['TELEGRAM_CHAT_ID']
        self.tg_bot = TgBotAPI(env_vars['TELEGRAM_LOOP_BOT'])

        self.market = market.upper()
        name = coin.upper().split('-')

        if self.market == 'COIN-FUTURES':
            self.coin = (name[0] + 'USD_' + name[1])
        elif self.market == 'USDT-FUTURES':
            self.coin = name[0] + 'USDT'
        else: # Spot
            if 'USD' in name:
                self.coin = name[0] + 'USDT'
            else:
                self.coin = name[0] + name[1]

        self.qty = float(qty)
        self.side = side
        self.execution_minutes = float(execution_minutes)
        self.execution_freq_per_minute = float(execution_freq_per_minute)

        self.cont = True if (cont == 'True' or cont == 'true') else False

        # Check previous DB;
        self._check_db()

        # Check precision of the market
        self._check_precision()
        self.number_of_executions = 0

    def _check_precision(self):
        exchange_information = self.binance.rest_client.get_exchange_information()
        for info in exchange_information['symbols']:
            if info['symbol'] == self.coin:
                if 'FUTURES' in self.market:
                    self.precision = int(info['quantityPrecision'])
                    break
                elif self.market == 'SPOT':
                    minQty = str(float(info['filters'][2]['minQty']))
                    if minQty.split('.')[-1] == '0':
                        self.precision = 0
                    else:
                        self.precision = len(minQty.split('.')[-1])
                    break
        else:
            raise Exception(f'Cannot find coin {self.coin} in Binance!')

    def _check_db(self):
        try:
            connect = sqlite3.connect(f"TWAP_BINANCE_{''.join(self.coin.split('/'))}_{self.market}.db")

            c = connect.cursor()
            select = c.execute("SELECT * from execution")
            cols = list(map(lambda x: x[0], select.description))

            last_row = None
            for row in select:
                last_row = row
            connect.close()

            last_data = {}
            for i in range(len(last_row)):
                last_data[cols[i]] = last_row[i]

            if not self.cont or \
                    last_data['complete_flag'] or \
                    self.side.upper() != last_data['side'] or \
                    self.market != last_data['market']:
                self.executed_qty = 0
                self.avg_price = 0
                self.complete_flag = False
            else:
                self.executed_qty = last_data['executed_qty']
                self.avg_price = last_data['average_price']
                self.complete_flag = False

        except sqlite3.OperationalError as e:
            print('NO PREVIOUS DATABASE! START!')
            self.executed_qty = 0
            self.avg_price = 0
            self.complete_flag = False
