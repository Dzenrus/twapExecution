from twapExecution.exchanges.okex.okexClient import Client
from twapExecution.exchanges.okex.consts import *


class OkexSpotClient(Client):

    def __init__(self, api_key, api_secret_key, passphrase, use_server_time=False, test=False, first=False):
        Client.__init__(self, api_key, api_secret_key, passphrase, use_server_time, test, first)

    # query spot account info
    def get_account_info(self):
        return self._request_without_params(GET, SPOT_ACCOUNT_INFO)

    def get_trade_fee(self, symbol):
        return self._request_with_params(GET, SPOT_TRADE_FEE, {'instrument_id': symbol})

    def get_symbol_price_ticker(self, symbol):
        return self._request_without_params(GET, SPOT_SPECIFIC_TICKER + symbol + '/ticker')

    def get_trading_pair_info(self, symbol):

        for pair in self._request_without_params(GET, SPOT_DEPTH):
            if pair['instrument_id'] == symbol:
                return pair

    def place_market_order(self, symbol, side, quantity):
        params = {'type': 'market',
                  'instrument_id': symbol,
                  'side': side}

        if side.lower() == 'buy':
            notional = quantity * float(self.get_symbol_price_ticker(symbol)['last'])
            params['notional'] = notional
        elif side.lower() == 'sell':
            params['size'] = quantity

        return self._request_with_params(POST, SPOT_ORDER, params)

    def get_commission_rate(self, symbol, taker=True):
        params = {'instrument_id': symbol}

        field = 'taker' if taker else 'maker'
        return float(self._request_with_params(GET, SPOT_TRADE_FEE, params)[field])