from twapExecution.exchanges.okex.okexClient import Client
from twapExecution.exchanges.okex.consts import *


class OkexFuturesClient(Client):

    def __init__(self, api_key, api_secret_key, passphrase, use_server_time=False, test=False, first=False):
        Client.__init__(self, api_key, api_secret_key, passphrase, use_server_time, test, first)

    # query spot account info
    def get_account_info(self):
        return self._request_without_params(GET, FUTURE_ACCOUNTS)

    def get_open_positions(self):
        return self._request_without_params(GET, FUTURE_POSITION)

    def get_trade_fee(self, symbol):
        return self._request_with_params(GET, FUTURE_TRADE_FEE, {'underlying': symbol})

    def get_symbol_price_ticker(self, symbol):
        return self._request_without_params(GET, FUTURE_SPECIFIC_TICKER + symbol + '/ticker')

    def place_market_order(self, symbol, side, quantity):
        params = {'instrument_id': symbol,
                  'size': int(quantity),
                  'order_type': '4'}

        if side.upper() == 'LONG-BUY':
            type = '1'
        elif side.upper() == 'LONG-SELL':
            type = '3'
        elif side.upper() == 'SHORT-BUY':
            type = '4'
        elif side.upper() == 'SHORT-SELL':
            type = '2'

        params['type'] = type

        return self._request_with_params(POST, FUTURE_ORDER, params)

    def post_change_initial_margin(self, symbol, leverage):
        params = {'leverage': str(leverage)}

        return self._request_with_params(POST, FUTURE_SET_LEVERAGE + '-'.join(symbol.split('-')[:-1]).lower() + '/leverage',
                                                                                    params)


    def get_commission_rate(self, symbol, taker=True):
        params = {'underlying': '-'.join(symbol.split('-')[:2])}

        field = 'taker' if taker else 'maker'
        return float(self._request_with_params(GET, FUTURE_TRADE_FEE, params)[field])