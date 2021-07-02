import argparse
import math
import sqlite3
import time
from datetime import datetime

import numpy as np
from telegram import ParseMode

from twapExecution.exchanges.database.databaseTWAP import execute
from twapExecution.exchanges.env import env_vars
from twapExecution.exchanges.huobi.huobiWSManager import HuobiWSManager
from twapExecution.exchanges.utils.utils import compute_rolling_average_price_and_qty
from twapExecution.tgBot.tgBotAPI import TgBotAPI


class HuobiSpotTWAP:
    def __init__(self, exchange, market, coin, qty, side, execution_minutes, execution_freq_per_minute, cont=True):
        print(exchange)
        self.huobi = HuobiWSManager(market)

        # tg bot
        self.chat_id = env_vars['TELEGRAM_CHAT_ID']
        self.tg_bot = TgBotAPI(env_vars['TELEGRAM_LOOP_BOT'])

        self.market = market.upper()

        name = coin.upper().split('-')

        if 'USD' in name:
            self.coin = name[0] + 'USDT'
        else:
            self.coin = name[0] + name[1]

        self.qty = float(qty)
        self.side = side
        self.execution_minutes = float(execution_minutes)
        self.execution_freq_per_minute = float(execution_freq_per_minute)

        self.cont = True if (cont == 'True' or cont == 'true') else False

        self._check_db()
        self.number_of_executions = 0

    def _check_db(self):
        try:
            connect = sqlite3.connect(f"TWAP_HUOBI_{''.join(self.coin.replace('-', ''))}_{self.market}.db")

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

    def _handle_message(self, message):
        if 'data' in message:
            if 'web' not in message['data']['orderSource']:
                if message['data']['orderStatus'] == 'filled' or message['data']['orderStatus'] == 'partial-filled':
                    self.avg_price, self.executed_qty = compute_rolling_average_price_and_qty(self.avg_price,
                                                                                              self.executed_qty,
                                                                                              float(message['data'][
                                                                                                        'tradeVolume']),
                                                                                              float(message['data'][
                                                                                                        'tradePrice']))
                    self.output_string = "{:.2f}/{:.2f} @ ${:.4f}".format(self.executed_qty,
                                                                          self.qty,
                                                                          self.avg_price)

                    print(self.output_string)
                    if round((self.qty - self.executed_qty), 8) == 0:
                        self.complete_flag = True

                    execute(db_name=f"TWAP_HUOBI_{self.coin.replace('-', '')}_SPOT",
                            exchange='HUOBI',
                            market=self.market,
                            order_id=message['data']['orderId'],
                            symbol=self.coin,
                            time=datetime.fromtimestamp(message['data']['tradeTime'] / 1e3).strftime(
                                '%Y-%m-%d %H:%M:%S'),
                            price=float(message['data']['tradePrice']),
                            side=self.side,
                            qty=float(message['data']['tradeVolume']),
                            overall_average=self.avg_price,
                            remaining_qty=self.qty - self.executed_qty,
                            executed_qty=self.executed_qty,
                            complete_flag=self.complete_flag)

                    if self.number_of_executions % 10 == 0 or round(self.executed_qty, 3) >= self.qty:
                        self.tg_bot.send_message(self.chat_id,
                                                 message='<code>' +
                                                         f"Huobi {self.market.capitalize()} executed {self.number_of_executions} trades\n" + \
                                                         f"{self.coin}: " + self.output_string + '</code>',
                                                 parse_mode=ParseMode.HTML)

    def run(self):
        self.huobi.start_user_stream(symbol=self.coin, callback=self._handle_message)
        account_id = self.huobi.rest_client.get_accounts()['data'][0]['id']

        while self.executed_qty < self.qty:
            id = self.tg_bot.get_updates()['result'][-1]['update_id']
            update = self.tg_bot.get_updates(id)['result'][0]

            try:
                if update['message']['text'] == f'/stop huobi {self.market.lower()}':
                    break

            except KeyError:
                print('No update yet!')
            except IndexError:
                print('No update yet!')

            try:
                time.sleep(60 / self.execution_freq_per_minute)

                order_size = (self.qty / self.execution_minutes) / self.execution_freq_per_minute
                order_size = round(np.random.uniform(order_size * 0.9, order_size * 1.1), 8)

                # TODO: EXTREMELY SMALL VALUE HANDLING
                if self.executed_qty + order_size > self.qty:
                    order_size = math.floor((self.qty - self.executed_qty) * 1e8) / 1e8

                if order_size != 0:
                    self.number_of_executions += 1
                    order = self.huobi.rest_client.post_new_market_order(id=account_id,
                                                                         symbol=self.coin,
                                                                         side=self.side,
                                                                         quantity=order_size)

                    if order['status'] == 'error':
                        print(order)
                        self.number_of_executions -= 1

                        # self.tg_bot.send_message(self.chat_id, message='<code>' + str(order) + '</code>',
                        #                          parse_mode=ParseMode.HTML)
                        # TODO: BETTER ERROR HANDLING
                        self.tg_bot.send_message(self.chat_id,
                                                 message='<code>' + f'Executed {self.number_of_executions} trades \n' +
                                                         self.output_string + '</code>',
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
                                         f"Huobi {self.market.capitalize()} executed {self.number_of_executions} trades\n" + \
                                         f"{self.coin}: " + self.output_string + '</code>', parse_mode=ParseMode.HTML)

        self.huobi.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Huobi Spot')
    parser.add_argument('--args', nargs='+', help='')

    args = parser.parse_args()

    # print(args.args)
    twap = HuobiSpotTWAP(*args.args)

    try:
        twap.run()
    except:
        twap.huobi.close()
