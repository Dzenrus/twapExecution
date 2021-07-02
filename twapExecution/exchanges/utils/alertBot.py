import argparse
import time

from telegram import ParseMode

from twapExecution.exchanges.binance.binanceClient import BinanceClient
from twapExecution.exchanges.env import env_vars
from twapExecution.tgBot.tgBotAPI import TgBotAPI


class Alert:
    def __init__(self,
                 coin,
                 price_threshold
                 ):
        self._coin = coin.upper()
        self._price_threshold = float(price_threshold)

        self._chat_id = env_vars['TELEGRAM_CHAT_ID']
        self._tg_bot = TgBotAPI(env_vars['TELEGRAM_LOOP_BOT'])
        self.rest_client = BinanceClient(env_vars['BINANCE_API_KEY'],
                                         env_vars['BINANCE_SECRET_KEY'])

    def run(self):
        current_price_not_in_range = True

        while current_price_not_in_range:
            current_price = int(float(self.rest_client.get_symbol_price_ticker(self._coin)['price']))
            # print('CURRENT PRICE', current_price)
            if ((current_price * 1.0005) >= self._price_threshold) and (
                    (current_price * 0.9995) <= self._price_threshold):
                # print('IT IS IN HERE NOW')
                self._tg_bot.send_message(self._chat_id,
                                          message='<code>' +
                                                  f"*******************************\n*******************************\n*******************************\n\n{self._coin} is approaching {self._price_threshold}\n\n*******************************\n*******************************\n*******************************\n" +
                                                  '</code>',
                                          parse_mode=ParseMode.HTML)
                current_price_not_in_range = False
                break
            time.sleep(15)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='----ALERT BOT----')
    parser.add_argument('--args', nargs='+', help='')

    args = parser.parse_args()

    # print(args.args)
    alert = Alert(*args.args)
    try:
        alert.run()
    except:
        alert.ws.close()
    print('DONE!!!!!')
