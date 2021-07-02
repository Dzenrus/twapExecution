import argparse
import calendar
import sqlite3
import time
from datetime import datetime

from telegram import ParseMode

from twapExecution.exchanges.database.databaseTWAP import execute
from twapExecution.exchanges.deribit.deribitWSManager import DeribitWSManager
from twapExecution.exchanges.env import env_vars
from twapExecution.exchanges.utils.utils import compute_rolling_average_price_and_qty
from twapExecution.tgBot.tgBotAPI import TgBotAPI


class DeribitTWAP:
    def __init__(self, exchange, market, coin, qty, side, execution_minutes, execution_freq_per_minute, cont=True,
                 account='SUB1'):

        self.account = account
        self.deribit = DeribitWSManager(account)

        # tg bot
        self.chat_id = env_vars['TELEGRAM_CHAT_ID']
        self.tg_bot = TgBotAPI(env_vars['TELEGRAM_LOOP_BOT'])

        self.market = market.upper()

        if 'PERP' in coin.upper():
            self.coin = coin.upper() + 'ETUAL'
        else:
            name = coin.upper().split('-')
            y, m, d = name[-1][0:2], name[-1][2:4], name[-1][4:6]
            m = calendar.month_name[int(m)][:3].upper()

            self.coin = name[0] + '-' + d + m + y

        print(self.coin)

        self.qty = float(qty) * 10
        self.side = side.upper()
        self.execution_minutes = float(execution_minutes)
        self.execution_freq_per_minute = float(execution_freq_per_minute)

        self.cont = True if (cont == 'True' or cont == 'true') else False

        self._check_db()
        self.number_of_executions = 0

    def _check_db(self):
        try:
            connect = sqlite3.connect(f"TWAP_DERIBIT_{''.join(self.coin.replace('-', ''))}_{self.market}.db")

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
        if message.get('method') == 'subscription':
            for i, msg in enumerate(message['params']['data']):
                if msg['state'] == 'filled':
                    self.avg_price, self.executed_qty = compute_rolling_average_price_and_qty(self.avg_price,
                                                                                              self.executed_qty,
                                                                                              float(msg['amount']),
                                                                                              float(msg['price']))
                    # /10 to display qty instead of amount
                    self.output_string = "{:.2f}/{:.2f} @ ${:.4f}".format(self.executed_qty / 10,
                                                                          self.qty / 10,
                                                                          self.avg_price)

                    print(self.output_string)
                    if round((self.qty - self.executed_qty), 8) == 0:
                        self.complete_flag = True

                    execute(db_name=f"TWAP_DERIBIT_{self.coin.replace('-', '')}_{self.market}",
                            exchange='DERIBIT',
                            market=self.market,
                            order_id=msg['order_id'],
                            symbol=self.coin,
                            time=datetime.fromtimestamp(msg['timestamp'] / 1e3).strftime(
                                '%Y-%m-%d %H:%M:%S'),
                            price=float(msg['price']),
                            side=self.side,
                            qty=float(msg['amount']),
                            overall_average=self.avg_price,
                            remaining_qty=self.qty - self.executed_qty,
                            executed_qty=self.executed_qty,
                            complete_flag=self.complete_flag)

                    if (self.number_of_executions % 10 == 0 and i == len(message['params']['data']) - 1) \
                            or round(self.executed_qty, 3) >= self.qty:
                        self.tg_bot.send_message(self.chat_id,
                                                 message='<code>' +
                                                         f"Deribit {self.market.capitalize()} {self.account.capitalize()} executed {self.number_of_executions} trades\n" + \
                                                         f"{self.coin}: " + self.output_string + '</code>',
                                                 parse_mode=ParseMode.HTML)

    def run(self):
        self.deribit.start_user_stream(symbol=self.coin, callback=self._handle_message)

        while self.executed_qty < self.qty:
            id = self.tg_bot.get_updates()['result'][-1]['update_id']
            update = self.tg_bot.get_updates(id)['result'][0]

            try:
                if update['message']['text'] == f'/stop deribit {self.market.lower()}':
                    break

            except KeyError:
                print('No update yet!')
            except IndexError:
                print('No update yet!')

            try:
                time.sleep((60 / self.execution_freq_per_minute) / 4)

                order_size = (self.qty / self.execution_minutes) / self.execution_freq_per_minute
                order_size = order_size - order_size % 10

                # TODO: EXTREMELY SMALL VALUE HANDLING
                if self.executed_qty + order_size > self.qty:
                    order_size = self.qty - self.executed_qty

                if order_size != 0:
                    self.number_of_executions += 1
                    if self.side == 'BUY':
                        order = self.deribit.rest_client.buy(instrument_name=self.coin,
                                                             amount=order_size,
                                                             order_type='market',
                                                             reduce_only=False)

                    elif self.side == 'SELL':
                        order = self.deribit.rest_client.sell(instrument_name=self.coin,
                                                              amount=order_size,
                                                              order_type='market',
                                                              reduce_only=False)

                    if 'error' in order:
                        print(order)
                        self.number_of_executions -= 1
                        # self.tg_bot.send_message(self.chat_id, message='<code>' + str(order) + '</code>',
                        #                          parse_mode=ParseMode.HTML)
                        # TODO: BETTER ERROR HANDLING
                        self.tg_bot.send_message(self.chat_id,
                                                 message='<code>' +
                                                         f"Deribit {self.market.capitalize()} {self.account.capitalize()} executed {self.number_of_executions} trades\n" + \
                                                         f"{self.coin}: " + self.output_string + '</code>',
                                                 parse_mode=ParseMode.HTML)
                        break
                else:
                    break

            except Exception as e:
                print(f'CANNOT PLACE AN ORDER! ERROR: {e}')
                self.tg_bot.send_message(chat_id=self.chat_id,
                                         message='<code>' + self.account.capitalize() + ': ' + e + '</code>',
                                         parse_mode=ParseMode.HTML)
                break

        self.tg_bot.send_message(chat_id=self.chat_id,
                                 message='<code>' +
                                         f"--------------------------------\nTWAP Stopped\n--------------------------------\n"
                                         f"Deribit {self.market.capitalize()} {self.account.capitalize()} executed {self.number_of_executions} trades\n" + \
                                         f"{self.coin}: " + self.output_string + '</code>', parse_mode=ParseMode.HTML)

        self.deribit.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Deribit Futures')
    parser.add_argument('--args', nargs='+', help='')

    args = parser.parse_args()

    print(args.args)
    twap = DeribitTWAP(*args.args)

    try:
        twap.run()
    except:
        twap.deribit.close()
