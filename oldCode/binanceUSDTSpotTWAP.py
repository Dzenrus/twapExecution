import argparse
import math
import time
from datetime import datetime

import numpy as np
from telegram import ParseMode

from twapExecution.exchanges.database.databaseTWAP import execute
from oldCode.binanceBaseTWAP import BinanceSimpleTWAP
from twapExecution.exchanges.utils.utils import compute_rolling_average_price_and_qty


class BinanceSpotTWAP(BinanceSimpleTWAP):

    def __init__(self, exchange, market, coin, qty, side, execution_minutes, execution_freq_per_minute, cont='True'):
        super().__init__(exchange,
                         market,
                         coin,
                         qty,
                         side,
                         execution_minutes,
                         execution_freq_per_minute,
                         cont)

    def _handle_message(self, message):
        if message['e'] == 'executionReport':
            # Ignore web orders
            if not message['c'].startswith('web'):
                if message['X'] == 'FILLED' or message['X'] == 'PARTIALLY_FILLED':
                    # L AND l = LAST EXECUTED PRICE AND LAST EXECUTED QTY;
                    self.avg_price, self.executed_qty = compute_rolling_average_price_and_qty(self.avg_price,
                                                                                              self.executed_qty,
                                                                                              float(message['l']),
                                                                                              float(message['L']))

                    self.output_string = "{:.2f}/{:.2f} @ {:.8f}".format(self.executed_qty,
                                                                         self.qty,
                                                                         self.avg_price)
                    print(self.output_string)
                    if round((self.qty - self.executed_qty), 8) == 0:
                        self.complete_flag = True

                    execute(db_name=f"TWAP_BINANCE_{self.coin}_{self.market}",
                            exchange='BINANCE',
                            market=self.market,
                            order_id=message['i'],
                            symbol=message['s'],
                            time=datetime.fromtimestamp(int(message['T']) / 1e3).strftime('%Y-%m-%d %H:%M:%S'),
                            price=message['L'],
                            side=message['S'],
                            qty=message['l'],
                            overall_average=self.avg_price,
                            remaining_qty=self.qty - self.executed_qty,
                            executed_qty=self.executed_qty,
                            complete_flag=self.complete_flag)

                    if message['X'] == 'FILLED':
                        print(self.executed_qty, self.qty, self.number_of_executions)
                        if self.number_of_executions % 10 == 0 or round(self.executed_qty, 3) >= self.qty:
                            self.tg_bot.send_message(self.chat_id,
                                                     message='<code>' +
                                                             f"Binance {self.market.capitalize()} executed {self.number_of_executions} trades\n" + \
                                                             f"{self.coin}: " + self.output_string + '</code>',
                                                     parse_mode=ParseMode.HTML)

    def run(self):
        self.binance.start_user_stream(self._handle_message)

        while self.executed_qty < self.qty:
            # get the first id;
            id = self.tg_bot.get_updates()['result'][-1]['update_id']
            # offset it by the id, to remove old updates
            update = self.tg_bot.get_updates(id)['result'][0]

            try:
                if update['message']['text'] == f'/stop binance {self.market.lower()}':
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
                    order_size = math.floor(
                        (self.qty - self.executed_qty) * (1 * 10 ** self.precision) / (1 * 10 ** self.precision))

                if order_size != 0:
                    self.number_of_executions += 1
                    order = self.binance.rest_client.post_new_market_order(symbol=self.coin,
                                                                           side=self.side.upper(),
                                                                           quantity=order_size,
                                                                           )
                    if 'code' in order:
                        self.number_of_executions -= 1
                        # print(order)
                        # self.tg_bot.send_message(chat_id=self.chat_id,
                        #                          message='<code>' + str(order) + '</code>',
                        #                          parse_mode=ParseMode.HTML)
                        self.tg_bot.send_message(self.chat_id,
                                                 message='<code>' +
                                                         f"Binance {self.market.capitalize()} executed {self.number_of_executions} trades\n" + \
                                                         f"{self.coin}: " + self.output_string + '</code>',
                                                 parse_mode=ParseMode.HTML)
                        break
                else:
                    break

            except Exception as e:
                print(f'CANNOT PLACE AN ORDER! ERROR: {e}')
                self.tg_bot.send_message(chat_id=self.chat_id, message='<code>' + str(e) + '</code>',
                                         parse_mode=ParseMode.HTML)
                break

        self.tg_bot.send_message(chat_id=self.chat_id,
                                 message='<code>' +
                                         f"--------------------------------\nTWAP Stopped\n--------------------------------\n"
                                         f"Binance {self.market.capitalize()} executed {self.number_of_executions} trades\n" + \
                                         f"{self.coin}: " + self.output_string + '</code>', parse_mode=ParseMode.HTML)

        self.binance.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Binance Spot')
    parser.add_argument('--args', nargs='+', help='')

    args = parser.parse_args()

    # print(args.args)
    twap = BinanceSpotTWAP(*args.args)

    try:
        twap.run()
    except:
        twap.binance.close()
