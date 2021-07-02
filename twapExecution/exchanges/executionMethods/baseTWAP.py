import argparse
import calendar
import sqlite3
import time

from telegram import ParseMode
from datetime import datetime
from twapExecution.exchanges.binance.binanceWSManager import BinanceWSManager
from twapExecution.exchanges.coinbase.coinbaseWSManager import CoinbaseWSManager
from twapExecution.exchanges.database.databaseTWAP import execute
from twapExecution.exchanges.deribit.deribitWSManager import DeribitWSManager
from twapExecution.exchanges.env import env_vars
from twapExecution.exchanges.executionMethods.orderManager import OrderManager
from twapExecution.exchanges.executionMethods.preprocessMsg import PreprocessMsg
from twapExecution.exchanges.okex.okexWSManager import OkexWSManager
from twapExecution.exchanges.utils.utils import compute_rolling_average_price_and_qty, compute_precision
from twapExecution.tgBot.tgBotAPI import TgBotAPI


class TWAP:
    def __init__(self,
                 exchange,
                 market,
                 coin,
                 qty,
                 price_threshold,
                 side,
                 execution_minutes,
                 execution_freq_per_minute,
                 cont,
                 leverage=1,
                 account='SUB1'):
        self._exchange = exchange.upper()
        self._market = market.upper()
        self._coin = coin.upper()
        self._input_coin_name = coin.upper()

        self._qty = float(qty)
        self._price_threshold = float(price_threshold)
        self._side = side.upper()
        self._execution_minutes = float(execution_minutes)
        self._execution_freq_per_minute = float(execution_freq_per_minute)
        self._cont = True if (cont == 'True' or cont == 'true') else False
        self._leverage = leverage
        self._account = account.upper()
        self._output_account = f' {self._account} ' if self._exchange == 'DERIBIT' else ' '

        self._output_string = ''

        self.fee_modification = 'DENOMINATOR'

        if self._exchange == 'BINANCE':
            self.ws = BinanceWSManager(self._market)

            name = self._coin.split('-')

            if self._market == 'COIN-FUTURES':
                self._coin = (name[0] + 'USD_' + name[1])
                self._price_name = self._coin
                self._commission = self.ws.rest_client.get_commission_rate(self._coin)

            elif self._market == 'USDT-FUTURES':
                self._coin = name[0] + 'USDT'
                self._price_name = self._coin
                self._commission = self.ws.rest_client.get_commission_rate(self._coin)

            else:  # Spot
                self._commission = self.ws.rest_client.get_commission_rate(self._coin)

                # TODO: Check For BNB sufficiency later;
                bnb_amount = 0
                balances = self.ws.rest_client.get_account_info()['balances']
                for asset in balances:
                    if asset['asset'] == 'BNB':
                        bnb_amount = float(asset['free'])
                        break
                if bnb_amount <= 1:
                    self.fee_modification = 'NUMERATOR'
                elif self._coin.startswith('BNB'):
                    self.fee_modification = 'NUMERATOR'
                    self._commission *= 0.75  # BNB Discount
                else:
                    self._commission *= 0.75  # BNB Discount

                if 'USD' in name:
                    self._coin = name[0] + 'USDT'
                    self._price_name = self._coin
                else:
                    self._coin = name[0] + name[1]
                    self._price_name = name[0] + 'USDT'

            if self._market == 'COIN-FUTURES' or self._market == 'USDT-FUTURES':
                print('CHANGE POSITION MODE')
                self.ws.rest_client.post_change_initial_margin(self._coin, self._leverage)
                print(self.ws.rest_client.post_position_mode('true'))

            self.ws.start_user_stream(self._handle_message)

        elif self._exchange == 'OKEX':
            self.ws = OkexWSManager(self._market)
            name = self._coin.split('-')

            if 'SPOT' in self._market:
                self.fee_modification = 'NUMERATOR'
                if 'USD' in name:
                    self._coin = name[0] + '-USDT'
                    self._price_name = self._coin
                else:
                    self._coin = name[0] + '-' + name[1]
                    self._price_name = name[0] + '-USDT'

            elif 'FUTURES' in self._market:
                if 'COIN' in self._market:
                    quote = 'USD'
                elif 'USDT' in self._market:
                    quote = 'USDT'
                self._coin = name[0] + f'-{quote}-' + name[1]
                self._price_name = self._coin
                self.ws.rest_client.post_change_initial_margin(self._coin, self._leverage)

            self._commission = self.ws.rest_client.get_commission_rate(self._coin)

            self.ws.start_user_stream(symbol=self._coin, callback=self._handle_message)

        elif self._exchange == 'COINBASE':
            self.ws = CoinbaseWSManager()
            name = self._coin.split('-')
            self._price_name = name[0] + '-USD'
            self._commission = self.ws.rest_client.get_commission_rate(self._coin)

            self.ws.start_user_stream(symbol=[self._coin], callback=self._handle_message)

        elif self._exchange == 'DERIBIT':
            self.ws = DeribitWSManager(self._account)
            self._qty = float(self._qty) * 10
            if 'PERP' in self._coin:
                self._coin = self._coin + 'ETUAL'
            else:
                name = self._coin.split('-')
                y, m, d = name[-1][0:2], name[-1][2:4], name[-1][4:6]
                m = calendar.month_name[int(m)][:3].upper()

                self._coin = name[0] + '-' + d + m + y
            self._price_name = self._coin
            self._commission = self.ws.main_client.get_commission_rate(self._coin)
            # self._commission = 0.01
            self.ws.start_user_stream(symbol=self._coin, callback=self._handle_message)

        # Compute Precision
        print('COMPUTE PERCISION')
        self._precision = compute_precision(exchange=self._exchange,
                                            market=self._market,
                                            coin=self._coin,
                                            client=self.ws.public_client if self._exchange == 'COINBASE'
                                            else self.ws.rest_client)
        print(f'PRECISION: {self._precision}')

        self._order_manager = OrderManager(self._exchange,
                                           self._market,
                                           self._coin,
                                           self._qty,
                                           self._side,
                                           self._execution_minutes,
                                           self._execution_freq_per_minute,
                                           self._precision)

        self._chat_id = env_vars['TELEGRAM_CHAT_ID']
        self._tg_bot = TgBotAPI(env_vars['TELEGRAM_LOOP_BOT'])

        self._preprocessor = PreprocessMsg(exchange=self._exchange,
                                           market=self._market)

        # Check previous DB;
        self._check_db()
        self._sent_message = None
        self._sent_message2 = None
        self._number_of_executions = 0
        self._repeated_n_times = 0

    def _check_db(self):
        try:
            connect = sqlite3.connect(f"TWAP_{self._exchange}_{self._input_coin_name}_{self._market}_{self._output_account}.db")
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

            if not self._cont or \
                    last_data['complete_flag'] or \
                    self._side != last_data['side'] or \
                    self._market != last_data['market']:
                self._executed_qty = 0
                self._avg_price = 0
                self._complete_flag = False
            else:
                print('CONTINUE LAST EXECUTION!!!')
                self._executed_qty = last_data['executed_qty']
                self._avg_price = last_data['average_price']
                self._complete_flag = False

        except sqlite3.OperationalError as e:
            print('NO PREVIOUS DATABASE! START!')
            self._executed_qty = 0
            self._avg_price = 0
            self._complete_flag = False

    def _handle_message(self, message):
        msg = self._preprocessor.handle_msg(message)
        # L AND l = LAST EXECUTED PRICE AND LAST EXECUTED QTY;
        if msg['price'] is not None and msg['symbol'] == self._coin:
            self._avg_price, self._executed_qty = compute_rolling_average_price_and_qty(self._avg_price,
                                                                                        self._executed_qty,
                                                                                        msg['qty'],
                                                                                        msg['price'])

            self._output_string = "{:.2f}/{:.2f} @ {:.8f}".format(self._executed_qty,
                                                                  self._qty,
                                                                  self._avg_price)
            print(self._output_string)
            if round((self._qty - self._executed_qty), 8) == 0:
                self._complete_flag = True

            execute(db_name=f"TWAP_{self._exchange}_{self._input_coin_name}_{self._market}_{self._output_account}",
                    exchange=self._exchange,
                    market=self._market,
                    order_id=msg['order_id'],
                    symbol=msg['symbol'],
                    time=msg['time'],
                    price=msg['price'],
                    side=self._side,
                    qty=msg['qty'],
                    overall_average=self._avg_price,
                    remaining_qty=self._qty - self._executed_qty,
                    executed_qty=self._executed_qty,
                    complete_flag=self._complete_flag)

        if msg['last']:
            if self._number_of_executions % 10 == 0 or round(self._executed_qty, 3) >= self._qty:
                if self._sent_message:
                    self._tg_bot.edit_message(
                        chat_id=self._sent_message['result']['chat']['id'],
                        message_id=self._sent_message['result']['message_id'],
                        message='<code>' + f"{self._side.capitalize()} " + f"{self._exchange.capitalize()} {self._market.capitalize()}{self._output_account.capitalize()}executed {self._number_of_executions} trades\n"
                                + f"{self._coin}: " + self._output_string + '</code>',
                        parse_mode=ParseMode.HTML)
                else:
                    self._sent_message = self._tg_bot.send_message(self._chat_id,
                                                                   message='<code>' + f"{self._side.capitalize()} " + f"{self._exchange.capitalize()} {self._market.capitalize()}{self._output_account.capitalize()}executed {self._number_of_executions} trades\n" + f"{self._coin}: " + self._output_string + '</code>',
                                                                   parse_mode=ParseMode.HTML)

    def run(self):
        self.execution_interval = 60 / self._execution_freq_per_minute

        while self._executed_qty < self._qty:
            next_time = time.time() + self.execution_interval

            print(f'**********Current Time 1: {datetime.now()}')
            print(f'Executed Qty: {self._executed_qty}, Qty: {self._qty}')
            # get the first id;
            id = self._tg_bot.get_updates()['result'][-1]['update_id']
            # offset it by the id, to remove old updates
            update = self._tg_bot.get_updates(id)['result'][0]

            try:
                if update['message']['text'] == f'/stop {self._exchange.lower()} {self._market.lower()}':
                    break

            except KeyError:
                print('No update yet!')
            except IndexError:
                print('No update yet!')

            try:
                if self._exchange == 'BINANCE':
                    price_info = self.ws.rest_client.get_symbol_price_ticker(self._price_name)
                    price_info = price_info if isinstance(price_info, dict) else price_info[0]
                    self._cur_price = float(price_info['price'])
                elif self._exchange == 'OKEX':
                    self._cur_price = float(self.ws.rest_client.get_symbol_price_ticker(self._price_name)['last'])
                elif self._exchange == 'COINBASE':
                    self._cur_price = float(self.ws.public_client.get_product_ticker(self._price_name)['price'])
                elif self._exchange == 'DERIBIT':
                    self._cur_price = float(self.ws.rest_client.ticker(self._price_name)['result']['mark_price'])
                    print(self._cur_price)

                enter = self._cur_price < self._price_threshold if 'BUY' in self._side else self._cur_price > self._price_threshold

                if enter:
                    self._order_manager.set_order_size(executed_qty=self._executed_qty, current_price=self._cur_price)
                    print(self._order_manager.order_size)

                    if self._order_manager.order_size > 0:
                        print('ENTER!')
                        self._number_of_executions += 1
                        order = self.ws.rest_client.place_market_order(**self._order_manager.market_order_kwargs())
                        print(order)
                        #
                        if self._order_manager.error_or_not(order):
                            self._number_of_executions -= 1
                            if self._sent_message:
                                self._tg_bot.edit_message(
                                    chat_id=self._sent_message['result']['chat']['id'],
                                    message_id=self._sent_message['result']['message_id'],
                                    message='<code>' + f"{self._side.capitalize()} " +
                                            f"{self._exchange.capitalize()} {self._market.capitalize()}{self._output_account.capitalize()}executed {self._number_of_executions} trades\n" + f"{self._coin}: " + self._output_string + '</code>',
                                    parse_mode=ParseMode.HTML)
                            # self._tg_bot.send_message(chat_id=self._chat_id, message='<code>' + str(order) + '</code>',
                            #                           parse_mode=ParseMode.HTML)
                            break
                    else:
                        break
                else:
                    print('NOT WITHIN THRESHOLD, WAIT!')

                    if 'BUY' in self._side:
                        output = f"Current price {self._cur_price} &#62; {self._price_threshold}"
                    else:
                        output = f"Current price {self._cur_price} &#60; {self._price_threshold}"
                    self._repeated_n_times += 1

                    if self._sent_message2:
                        self._tg_bot.edit_message(
                            chat_id=self._sent_message2['result']['chat']['id'],
                            message_id=self._sent_message2['result']['message_id'],
                            message='<code>' + f"{self._side.capitalize()} " + f"{self._exchange.capitalize()} {self._market.capitalize()}{self._output_account.capitalize()}repeated {self._repeated_n_times} times\n"
                                    + f"{self._coin}: " + self._output_string + '\n' + output + '</code>',
                            parse_mode=ParseMode.HTML)
                    else:
                        print('SEND MSG!')
                        # print('<code>' + f"{self._side.capitalize()} " + f"{self._exchange.capitalize()} {self._market.capitalize()}{self._output_account.capitalize()}repeated {self._repeated_n_times} trades\n" + f"{self._coin}: " + output + '</code>')
                        self._sent_message2 = self._tg_bot.send_message(self._chat_id,
                                                                        message='<code>' + f"{self._side.capitalize()} " + f"{self._exchange.capitalize()} {self._market.capitalize()}{self._output_account.capitalize()}repeated {self._repeated_n_times} trades\n" + f"{self._coin}: " + self._output_string + '\n' + output + '</code>',
                                                                        parse_mode=ParseMode.HTML)
                        print(self._sent_message2)

                time_diff = next_time - time.time() if next_time - time.time() > 0 else 0
                print(f'**********Current Time 2: {datetime.now()}')
                print(f'SLEEP FOR {time_diff} seconds')
                time.sleep(time_diff)

            except Exception as e:
                print(f'CANNOT PLACE AN ORDER! ERROR: {e}')
                self._tg_bot.send_message(chat_id=self._chat_id, message='<code>' + str(e) + '</code>',
                                          parse_mode=ParseMode.HTML)
                break

        # DONE TWAP! STOPPED! PRINT LAST MESSAGE

        if self.fee_modification == 'NUMERATOR':
            self._output_string = "{:.2f}/{:.2f} @ {:.8f}".format(self._executed_qty * (1 - self._commission),
                                                                  self._qty * (1 - self._commission),
                                                                  self._avg_price)
        elif self.fee_modification == 'DENOMINATOR':
            self._output_string = "{:.2f}/{:.2f} @ {:.8f}".format(self._executed_qty,
                                                                  self._qty,
                                                                  self._avg_price * (
                                                                          1 + self._commission) if 'BUY' in self._side else self._avg_price * (
                                                                          1 - self._commission))

        self._tg_bot.send_message(self._chat_id,
                                  message='<code>' +
                                          f"-------------------------\nTWAP Stopped\n-------------------------\n" +
                                          f"{self._side.capitalize()} " +
                                          f"{self._exchange.capitalize()} "
                                          f"{self._market.capitalize()}"
                                          f"{self._output_account.capitalize()}"
                                          f"executed "
                                          f"{self._number_of_executions} trades\n" +
                                          f"{self._coin}: " +
                                          self._output_string +
                                          '</code>',
                                  parse_mode=ParseMode.HTML)

        self.ws.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='----TWAP EXECUTION----')
    parser.add_argument('--args', nargs='+', help='')

    args = parser.parse_args()

    # print(args.args)

    list_of_args = args.args

    twap = TWAP(*list_of_args)

    # temp solve 'USDT' problem.
    if 'USDT' not in list_of_args[2].upper():
        twap.run()
        # try:
        #     twap.run()
        # except Exception as e:
        #     print(e)
        #     twap.ws.close()
    else:
        twap.ws.close()

    print('DONE!!!!!')