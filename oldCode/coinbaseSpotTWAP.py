import argparse
import sqlite3
import time
from datetime import datetime

import numpy as np
from telegram import ParseMode

from twapExecution.exchanges.coinbase.coinbaseWSManager import CoinbaseWSManager
from twapExecution.exchanges.database.databaseTWAP import execute
from twapExecution.exchanges.env import env_vars
from twapExecution.exchanges.utils.utils import compute_rolling_average_price_and_qty
from twapExecution.tgBot.tgBotAPI import TgBotAPI


class CoinbaseTWAP:
    def __init__(self, exchange, market, coin, qty, side, execution_minutes, execution_freq_per_minute, cont=True):
        print(exchange)
        self.coinbase = CoinbaseWSManager()

        # tg bot
        self.chat_id = env_vars['TELEGRAM_CHAT_ID']
        self.tg_bot = TgBotAPI(env_vars['TELEGRAM_LOOP_BOT'])

        self.market = market.upper()
        self.coin = coin.upper()
        self.qty = float(qty)
        self.side = side
        self.execution_minutes = float(execution_minutes)
        self.execution_freq_per_minute = float(execution_freq_per_minute)

        self.cont = True if (cont == 'True' or cont == 'true') else False

        self._check_db()
        self._check_precision()
        self.number_of_executions = 0

    def _check_db(self):
        try:
            connect = sqlite3.connect(f"TWAP_COINBASE_{''.join(self.coin.replace('-', ''))}_{self.market}.db")

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

            if not self.cont or last_data['complete_flag'] or \
                    self.side != last_data['side'] or \
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

    def _check_precision(self):
        all_products = self.coinbase.public_client.get_products()

        for product in all_products:
            if product['id'] == self.coin:
                self.precision = len(product['base_increment'].split('.')[1].split('1')[0]) + 1
                break
        else:
            raise Exception(f'Cannot find coin {self.coin} in Coinbase!')

    def _handle_message(self, message):
        if 'type' in message:
            if message['type'] == 'match':
                self.avg_price, self.executed_qty = compute_rolling_average_price_and_qty(self.avg_price,
                                                                                          self.executed_qty,
                                                                                          float(message['size']),
                                                                                          float(message['price']))
                self.output_string = "{:.2f}/{:.2f} @ {:.8f}".format(self.executed_qty,
                                                                     self.qty,
                                                                     self.avg_price)

                print(self.output_string)
                if round((self.qty - self.executed_qty), 8) == 0:
                    self.complete_flag = True

                execute(db_name=f"TWAP_COINBASE_{self.coin.replace('-', '')}_SPOT",
                        exchange='COINBASE',
                        market=self.market,
                        order_id=message['taker_order_id'],
                        symbol=self.coin,
                        time=datetime.strptime(message['time'], "%Y-%m-%dT%H:%M:%S.%fZ").strftime(
                            '%Y-%m-%d %H:%M:%S'),
                        price=float(message['price']),
                        side=self.side,
                        qty=float(message['size']),
                        overall_average=self.avg_price,
                        remaining_qty=self.qty - self.executed_qty,
                        executed_qty=self.executed_qty,
                        complete_flag=self.complete_flag)

            elif message['type'] == 'done':
                if float(message['remaining_size']) == 0 and \
                        (self.number_of_executions % 10 == 0 or round(self.executed_qty, 3) >= self.qty):
                    self.tg_bot.send_message(self.chat_id,
                                             message='<code>' +
                                                     f"Coinbase {self.market.capitalize()} executed {self.number_of_executions} trades\n" + \
                                                     f"{self.coin}: " + self.output_string + '</code>',
                                             parse_mode=ParseMode.HTML)

    def run(self):
        self.coinbase.start_user_stream(symbol=[self.coin], callback=self._handle_message)

        while self.executed_qty < self.qty:
            id = self.tg_bot.get_updates()['result'][-1]['update_id']
            update = self.tg_bot.get_updates(id)['result'][0]

            try:
                if update['message']['text'] == f'/stop coinbase {self.market.lower()}':
                    break

            except KeyError:
                print('No update yet!')
            except IndexError:
                print('No update yet!')

            try:
                time.sleep(60 / self.execution_freq_per_minute)

                order_size = (self.qty / self.execution_minutes) / self.execution_freq_per_minute
                order_size = round(np.random.uniform(order_size * 0.9, order_size * 1.1), self.precision)

                if self.executed_qty + order_size > self.qty:
                    order_size = round(self.qty - self.executed_qty, 8)

                if order_size != 0:
                    self.number_of_executions += 1
                    order = self.coinbase.rest_client.place_market_order(product_id=self.coin,
                                                                         side=self.side,
                                                                         size=order_size)

                    if 'message' in order:
                        print(order)
                        self.number_of_executions -= 1
                        # self.tg_bot.send_message(self.chat_id, message='<code>' + str(order) + '</code>',
                        #                          parse_mode=ParseMode.HTML)
                        self.tg_bot.send_message(self.chat_id,
                                                 message='<code>' +
                                                         f"Coinbase {self.market.capitalize()} executed {self.number_of_executions} trades\n" + \
                                                         f"{self.coin}: " + self.output_string + '</code>',
                                                 parse_mode=ParseMode.HTML)
                        break
                else:
                    break

            except Exception as e:
                print(f'CANNOT PLACE AN ORDER! ERROR: {e}')
                self.tg_bot.send_message(chat_id=self.chat_id, message='<code>' + e + '</code>',
                                         parse_mode=ParseMode.HTML)
                break

        self.tg_bot.send_message(chat_id=self.chat_id,
                                 message='<code>' +
                                         f"--------------------------------\nTWAP Stopped\n--------------------------------\n"
                                         f"Coinbase {self.market.capitalize()} executed {self.number_of_executions} trades\n" + \
                                         f"{self.coin}: " + self.output_string + '</code>', parse_mode=ParseMode.HTML)

        self.coinbase.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Coinbase Spot')
    parser.add_argument('--args', nargs='+', help='')

    args = parser.parse_args()

    # print(args.args)
    twap = CoinbaseTWAP(*args.args)

    try:
        twap.run()
    except:
        twap.coinbase.close()
