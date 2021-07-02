import asyncio
import websockets
import json


# create a websocket object
class DeribitClient(object):
    def __init__(self, api_key=None, secret_key=None, server_url=None):
        self._api_key = api_key
        self._secret_key = secret_key
        self._server_url = server_url
        self.json = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": None,
        }

    # send an authentication request
    async def private_api(self, request):
        options = {
            "grant_type": "client_credentials",
            "client_id": self._api_key,
            "client_secret": self._secret_key
        }

        self.json["method"] = "public/auth"
        self.json["params"] = options

        async with websockets.connect(self._server_url) as ws:
            await ws.send(json.dumps(self.json))
            response = await ws.recv()

            while ws.open:
                await ws.send(request)
                response = await ws.recv()
                response = json.loads(response)
                break
            return response

    # send a public method request
    async def public_api(self, request):
        async with websockets.connect(self._server_url) as ws:
            await ws.send(request)
            response = await ws.recv()
            response = json.loads(response)
            return response

    # send a public subscription request
    async def public_sub(self, request):
        async with websockets.connect(self._server_url) as ws:
            await ws.send(request)
            while ws.open:
                response = await ws.recv()
                response = json.loads(response)
                print(response)

    # create an asyncio event loop
    def loop(self, api, request):
        response = asyncio.run(api(json.dumps(request)))
        return response

    def index(self, currency):
        options = {"currency": currency}
        self.json["method"] = "public/get_index"
        self.json["params"] = options
        return self.loop(self.public_api, self.json)

    def ticker(self, instrument_name):
        options = {"instrument_name": instrument_name}
        self.json["method"] = "public/ticker"
        self.json["params"] = options
        return self.loop(self.public_api, self.json)

    def buy(self, instrument_name, amount, order_type, reduce_only,
            price=None, post_only=None):
        options = {
            "instrument_name": instrument_name,
            "amount": amount,
            "type": order_type,
            "reduce_only": reduce_only,
        }

        if price:
            options["price"] = price
        if post_only:
            options["post_only"] = post_only

        self.json["method"] = "private/buy"
        self.json["params"] = options
        return self.loop(self.private_api, self.json)

    def stop_buy(self, instrument_name, trigger, amount, order_type, reduce_only,
                 stop_price=None, price=None):
        options = {
            "trigger": trigger,
            "instrument_name": instrument_name,
            "amount": amount,
            "type": order_type,
            "reduce_only": reduce_only,
        }

        if stop_price:
            options["stop_price"] = stop_price
        if price:
            options["price"] = price

        self.json["method"] = "private/buy"
        self.json["params"] = options
        return self.loop(self.private_api, self.json)

    def sell(self, instrument_name, amount, order_type, reduce_only,
             price=None, post_only=None):
        options = {
            "instrument_name": instrument_name,
            "amount": amount,
            "type": order_type,
            "reduce_only": reduce_only,
        }

        if price:
            options["price"] = price
        if post_only:
            options["post_only"] = post_only

        self.json["method"] = "private/sell"
        self.json["params"] = options

        return self.loop(self.private_api, self.json)

    def stop_sell(self, instrument_name, trigger, amount, order_type, reduce_only,
                  stop_price=None, price=None):
        options = {
            "trigger": trigger,
            "instrument_name": instrument_name,
            "amount": amount,
            "type": order_type,
            "reduce_only": reduce_only,
        }

        if stop_price:
            options["stop_price"] = stop_price
        if price:
            options["price"] = price

        self.json["method"] = "private/sell"
        self.json["params"] = options
        return self.loop(self.private_api, self.json)

    def place_market_order(self, instrument_name, amount, side, reduce_only=False):
        if side.upper() == 'BUY':
            return self.buy(instrument_name=instrument_name,
                            amount=amount,
                            order_type='market',
                            reduce_only=reduce_only)
        elif side.upper() == 'SELL':
            return self.sell(instrument_name=instrument_name,
                             amount=amount,
                             order_type='market',
                             reduce_only=reduce_only)

    def edit(self, order_id, amount, price):
        options = {
            "order_id": order_id,
            "amount": amount,
            "price": price
        }

        self.json["method"] = "private/edit"
        self.json["params"] = options
        return self.loop(self.private_api, self.json)

    def cancel(self, order_id):
        options = {"order_id": order_id}
        self.json["method"] = "private/cancel"
        self.json["params"] = options
        return self.loop(self.private_api, self.json)

    def cancel_all(self):
        self.json["method"] = "private/cancel_all"
        return self.loop(self.private_api, self.json)

    def account_summary(self, currency, extended=False):
        options = {"currency": currency, 'extended': extended}
        self.json["method"] = "private/get_account_summary"
        self.json["params"] = options
        return self.loop(self.private_api, self.json)

    def subaccount_summary(self, currency, with_portfolio=True):
        options = {"currency": currency, 'with_portfolio': with_portfolio}
        self.json["method"] = "private/get_subaccounts"
        self.json["params"] = options
        return self.loop(self.private_api, self.json)

    def get_position(self, instrument_name):
        options = {"instrument_name": instrument_name}
        self.json["method"] = "private/get_position"
        self.json["params"] = options
        return self.loop(self.private_api, self.json)

    def get_positions(self, currency):
        options = {"currency": currency}
        self.json["method"] = "private/get_positions"
        self.json["params"] = options
        return self.loop(self.private_api, self.json)

    def public_trades(self):
        options = {"channels": ["trades.BTC-PERPETUAL.raw"]}
        self.json["method"] = "public/subscribe"
        self.json["params"] = options
        return self.loop(self.public_sub, self.json)

    def chart(self):
        options = {"channels": ["chart.trades.BTC-PERPETUAL.1"]}
        self.json["method"] = "public/subscribe"
        self.json["params"] = options
        return self.loop(self.public_sub, self.json)

    def get_commission_rate(self, symbol, taker=True):
        field = 'taker_fee' if taker else 'maker_fee'
        return float(self.account_summary('BTC', True)['result']['fees'][0][field])
