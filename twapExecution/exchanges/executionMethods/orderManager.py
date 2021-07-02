import time
import numpy as np
import math


class OrderManager:
    def __init__(self, exchange, market, coin, qty, side, execution_minutes, execution_freq_per_minute,
                 precision):
        self._exchange = exchange
        self._market = market
        self._coin = coin
        self._qty = qty
        self._side = side
        self._execution_minutes = execution_minutes
        self._execution_freq_per_minute = execution_freq_per_minute
        self._precision = precision
        self._number_of_executions = self._execution_minutes * self._execution_freq_per_minute

        self.order_sizes = []

    def order_delay(self):
        time.sleep((60 / self._execution_freq_per_minute))

    def set_order_size(self, executed_qty, current_price):
        if self._exchange == 'BINANCE':
            print('COMPUTING BINANCE ORDER SIZE')
            self.order_size = (self._qty / self._execution_minutes) / self._execution_freq_per_minute
            self.order_size = round(np.random.uniform(self.order_size * 0.9, self.order_size * 1.1), self._precision)
            print('order size:', self.order_size)

            qty_after = executed_qty + self.order_size

            if self._market == 'SPOT' or self._market == 'USDT-FUTURES':
                remaining_qty = self._qty - qty_after

                threshold = 10 if 'SPOT' in self._market else 5

                if remaining_qty * current_price < threshold:
                    print(f'REMAINING: {remaining_qty}, Too little, executed all!')
                    self.order_size = round(self._qty - executed_qty, self._precision)
            else:
                if qty_after > self._qty:
                    self.order_size = round(self._qty - executed_qty, self._precision)

        elif self._exchange == 'COINBASE':
            self.order_size = (self._qty / self._execution_minutes) / self._execution_freq_per_minute
            self.order_size = round(np.random.uniform(self.order_size * 0.9, self.order_size * 1.1), self._precision)

            qty_after = executed_qty + self.order_size
            remaining_qty = self._qty - qty_after

            threshold = 10

            if remaining_qty * current_price < threshold:
                print(f'REMAINING: {remaining_qty}, Too little, executed all!')
                self.order_size = round(self._qty - executed_qty, self._precision)

        elif self._exchange == 'DERIBIT':
            self.order_size = (self._qty / self._execution_minutes) / self._execution_freq_per_minute
            # multiple of 10
            self.order_size = self.order_size - self.order_size % 10

            qty_after = executed_qty + self.order_size

            if qty_after > self._qty:
                self.order_size = round(self._qty - executed_qty, self._precision)

        elif self._exchange == 'OKEX':
            self.order_size = (self._qty / self._execution_minutes) / self._execution_freq_per_minute
            self.order_size = round(np.random.uniform(self.order_size * 0.9, self.order_size * 1.1), self._precision)
            print(f'ORDER SIZE: {self.order_size}')

            qty_after = executed_qty + self.order_size

            if self._market == 'SPOT':
                remaining_qty = self._qty - qty_after

                threshold = 10

                if remaining_qty * current_price < threshold:
                    print(f'REMAINING: {remaining_qty}, Too little, executed all!')
                    self.order_size = round(self._qty - executed_qty, self._precision)
                    print(f'NEW ORDER SIZE: {self.order_size}')

            else:
                if qty_after > self._qty:
                    # self.order_size = math.floor((self._qty - executed_qty) * 1e8) / 1e8
                    self.order_size = round(self._qty - executed_qty, self._precision)

    def market_order_kwargs(self):
        if self._exchange == 'BINANCE':
            if 'SPOT' in self._market:
                order_kwargs = {
                    'symbol': self._coin,
                    'side': self._side,
                    'quantity': self.order_size,
                    'positionSide': None
                }
            elif 'FUTURES' in self._market:
                position_side, side = self._side.split('-')
                order_kwargs = {
                    'symbol': self._coin,
                    'side': side,
                    'quantity': self.order_size,
                    'positionSide': position_side
                }
            return order_kwargs

        elif self._exchange == 'COINBASE':
            if self._market == 'SPOT':
                return {
                    'product_id': self._coin,
                    'side': self._side.lower(),
                    'size': self.order_size
                }
        elif self._exchange == 'DERIBIT':
            if self._market == 'FUTURES':
                return {
                    'instrument_name': self._coin,
                    'side': self._side.lower(),
                    'amount': self.order_size
                }
        elif self._exchange == 'OKEX':
            return {
                'symbol': self._coin,
                'side': self._side.lower(),
                'quantity': self.order_size
            }

    def limit_order_kwargs(self):
        pass

    def error_or_not(self, order):
        error = False
        if self._exchange == 'BINANCE':
            if 'code' in order:
                error = True

        elif self._exchange == 'COINBASE':
            if 'message' in order:
                error = True

        elif self._exchange == 'DERIBIT':
            if 'error' in order:
                error = True

        elif self._exchange == 'OKEX':
            if order['error_message']:
                print('OKEX ERROR!')
                error = True

        return error
