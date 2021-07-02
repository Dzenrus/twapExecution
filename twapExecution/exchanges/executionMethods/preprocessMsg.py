from datetime import datetime


class PreprocessMsg:
    def __init__(self, exchange, market):
        self._exchange = exchange.upper()
        self._market = market.upper()

        self.okex_futures_params = {'1': 'long-buy',
                                    '2': 'short-sell',
                                    '3': 'long-sell',
                                    '4': 'short-buy'}

    def handle_msg(self, message):
        if self._exchange == 'BINANCE':
            return self.binance(message)
        elif self._exchange == 'COINBASE':
            return self.coinbase(message)
        elif self._exchange == 'DERIBIT':
            return self.deribit(message)
        elif self._exchange == 'OKEX':
            return self.okex(message)

    def _construct_new_msg(self, symbol, price, qty, side, order_id, time, last):
        return dict(exchange=self._exchange,
                    market=self._market,
                    symbol=symbol,
                    price=price,
                    qty=qty,
                    side=side,
                    order_id=order_id,
                    time=time,
                    last=last)

    def binance(self, message):
        if 'SPOT' in self._market:
            if message['e'] == 'executionReport':
                if not message['c'].startswith('web'):
                    if message['X'] == 'FILLED' or message['X'] == 'PARTIALLY_FILLED':
                        return self._construct_new_msg(symbol=message['s'].upper(),
                                                       price=float(message['L']),
                                                       qty=float(message['l']),
                                                       side=message['S'].upper(),
                                                       order_id=message['i'],
                                                       time=datetime.fromtimestamp(int(message['T']) / 1e3).strftime(
                                                           '%Y-%m-%d %H:%M:%S'),
                                                       last=True if message['X'] == 'FILLED' else False)

        elif 'FUTURES' in self._market:
            print(message)
            if not message['o']['c'].startswith('web'):
                if message['o']['X'] == 'FILLED' or message['o']['X'] == 'PARTIALLY_FILLED':
                    return self._construct_new_msg(symbol=message['o']['s'].upper(),
                                                   price=float(message['o']['L']),
                                                   qty=float(message['o']['l']),
                                                   side=message['o']['S'].upper(),
                                                   order_id=message['o']['i'],
                                                   time=datetime.fromtimestamp(int(message['T']) / 1e3).strftime(
                                                       '%Y-%m-%d %H:%M:%S'),
                                                   last=True if message['o']['X'] == 'FILLED' else False
                                                   )

    def coinbase(self, message):
        if 'SPOT' in self._market:
            if 'type' in message:
                print(message)
                if message['type'] == 'match':
                    msg = self._construct_new_msg(symbol=message['product_id'],
                                                  price=float(message['price']),
                                                  qty=float(message['size']),
                                                  side='BUY' if message['side'].upper() == 'SELL' else 'SELL',
                                                  order_id=message['taker_order_id'],
                                                  time=datetime.strptime(message['time'],
                                                                         "%Y-%m-%dT%H:%M:%S.%fZ").strftime(
                                                      '%Y-%m-%d %H:%M:%S'),
                                                  last=False)
                    print(msg)
                    return msg
                elif message['type'] == 'done':
                    if float(message['remaining_size']) == 0:
                        msg = self._construct_new_msg(symbol=None,
                                                      price=None,
                                                      qty=None,
                                                      side=None,
                                                      order_id=None,
                                                      time=None,
                                                      last=True)
                        print(msg)
                        return msg

    def okex(self, message):
        if 'SPOT' in self._market:
            if 'data' in message:
                data = message['data'][0]
                if data['state'] == '2':
                    return self._construct_new_msg(symbol=data['instrument_id'],
                                                   price=float(data['filled_notional']) / float(
                                                       data['filled_size']),
                                                   qty=float(data['filled_size']),
                                                   side=data['side'].upper(),
                                                   order_id=data['order_id'],
                                                   time=datetime.strptime(data['timestamp'],
                                                                          "%Y-%m-%dT%H:%M:%S.%fZ").strftime(
                                                       '%Y-%m-%d %H:%M:%S'),
                                                   last=True)

        elif 'FUTURES' in self._market:
            if 'data' in message:
                data = message['data'][0]
                if data['state'] == '2':
                    return self._construct_new_msg(symbol=data['instrument_id'],
                                                   price=float(data['price_avg']),
                                                   qty=float(data['filled_qty']),
                                                   side=self.okex_futures_params[data['type']].upper(),
                                                   order_id=data['order_id'],
                                                   time=datetime.strptime(data['timestamp'],
                                                                          "%Y-%m-%dT%H:%M:%S.%fZ").strftime(
                                                       '%Y-%m-%d %H:%M:%S'),
                                                   last=True)

    def deribit(self, message):
        print(message)
        m = {}
        if 'FUTURES' in self._market:
            if message.get('method') == 'subscription':
                for i, msg in enumerate(message['params']['data']):
                    if msg['state'] == 'filled':
                        if not m:
                            m = self._construct_new_msg(symbol=msg['instrument_name'],
                                                        price=float(msg['price']),
                                                        qty=float(msg['amount']),
                                                        side=msg['direction'].upper(),
                                                        order_id=msg['order_id'],
                                                        time=datetime.fromtimestamp(msg['timestamp'] / 1e3).strftime(
                                                            '%Y-%m-%d %H:%M:%S'),
                                                        last=True)
                            # TODO: Limit order needs to redo last=True;
                        else:
                            original_price = m['price']
                            original_qty = m['qty']

                            m['qty'] = original_qty + float(msg['amount'])
                            m['price'] = ((original_price * original_qty) + (float(msg['amount'])*float(msg['price'])))/m['qty']

                return m
