import hashlib
import hmac
import json
import time
import urllib
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import requests
from aiohttp import ClientSession
import asyncio


def generate_binance_signature(secret_key, builder):
    """

    Parameters
    ----------
    secret_key: string; secret key of API
    builder: builder object; from URLParamsBuilder

    Returns
    -------

    """
    if secret_key is None or secret_key == "":
        raise Exception('please input a correct secret key!')

    query_string = builder.build_url()
    signature = hmac.new(secret_key.encode(), msg=query_string.encode(), digestmod=hashlib.sha256).hexdigest()
    builder.put_url("signature", signature)


class UrlParamsBuilder:

    def __init__(self):
        self.param_map = dict()
        self.post_map = dict()

    def put_url(self, name, value):
        if value is not None:
            if isinstance(value, list):
                self.param_map[name] = json.dumps(value)
            elif isinstance(value, float):
                # self.param_map[name] = ('%.20f' % (value))[slice(0, 16)].rstrip('0').rstrip('.')
                self.param_map[name] = str(value)
            else:
                self.param_map[name] = str(value)

    def put_post(self, name, value):
        if value is not None:
            if isinstance(value, list):
                self.post_map[name] = value
            else:
                self.post_map[name] = str(value)

    def build_url(self):
        if len(self.param_map) == 0:
            return ""
        encoded_param = urllib.parse.urlencode(self.param_map)
        return encoded_param

    def build_url_to_json(self):
        return json.dumps(self.param_map)


class RestApiRequest:

    def __init__(self):
        self.method = ""
        self.url = ""
        self.host = ""
        self.post_body = ""
        self.header = dict()
        self.json_parser = None
        self.header.update({"client_SDK_Version": "binance_futures-1.0.1-py3.7"})


class BinanceClient:
    def __init__(self, api_key, secret_key, server_url='https://fapi.binance.com'):
        """

        Parameters
        ----------
        api_key string; Binance API key
        secret_key: string; Binance secret key
        server_url: string; Binance API url
        """
        self._api_key = api_key
        self._secret_key = secret_key
        self._server_url = server_url
        self._api_name = self._server_url.split('.')[0][8:]
        self._api_version = 'v3' if self._api_name == 'api' else 'v1'

        print(f"CONNECTING TO SERVER: {self._server_url}")

    def _create_request(self, mode, url, builder):
        request = RestApiRequest()
        request.method = mode.upper()
        request.host = self._server_url
        request.header.update({"X-MBX-APIKEY": self._api_key})
        # request.header.update({'Content-Type': 'application/json'})
        request.url = url + "?" + builder.build_url()

        return request

    def _create_request_with_signature(self, mode, url, builder):
        request = RestApiRequest()
        request.method = mode.upper()
        request.host = self._server_url
        builder.put_url("recvWindow", 10000)
        builder.put_url("timestamp", str(int(round(time.time() * 1000))))
        generate_binance_signature(self._secret_key, builder)
        # request.header.update({"Content-Type": "application/json"})
        request.header.update({"X-MBX-APIKEY": self._api_key})
        request.url = url + "?" + builder.build_url()

        return request

    def get_server_time(self):
        """

        Returns
        -------
        response: dict;
        """

        builder = UrlParamsBuilder()

        r = self._create_request("GET", f'/{self._api_name}/{self._api_version}/time', builder)
        response = requests.request(r.method, r.host + r.url, headers=r.header).json()

        return response

    def get_exchange_information(self):
        """

        Returns
        -------
        response: dict;
        """

        builder = UrlParamsBuilder()

        r = self._create_request("GET", f"/{self._api_name}/{self._api_version}/exchangeInfo", builder)
        response = requests.request(r.method, r.host + r.url, headers=r.header).json()

        return response

    def get_commission_rate(self, symbol, taker=True):
        builder = UrlParamsBuilder()

        if self._api_name == 'api':
            r = self._create_request_with_signature("GET", f"/{self._api_name}/{self._api_version}/account", builder)
            response = requests.request(r.method, r.host + r.url, headers=r.header).json()
            field = 'takerCommission' if taker else 'makerCommission'
            return float(response[field] / 10000)

        else:
            builder.put_url("symbol", symbol)
            r = self._create_request_with_signature("GET", f"/{self._api_name}/{self._api_version}/commissionRate",
                                                    builder)
            response = requests.request(r.method, r.host + r.url, headers=r.header).json()
            field = 'takerCommissionRate' if taker else 'makerCommissionRate'
            return float(response[field])

    def get_all_symbols_with_great_volume(self, target_v):
        """

        Parameters
        ----------
        target_v: float; volume in Millons

        Returns
        -------
        symbols: list;
        """

        builder = UrlParamsBuilder()

        r = self._create_request("GET", f"/{self._api_name}/{self._api_version}/ticker/24hr", builder)
        response = requests.request(r.method, r.host + r.url, headers=r.header).json()

        symbols = []

        for pair in response:
            if pair['symbol'].endswith('USDT'):
                if float(pair['volume']) * float(pair['lastPrice']) > target_v:
                    if 'UP' not in pair['symbol'] and 'DOWN' not in pair['symbol'] and 'EUR' not in pair[
                        'symbol'] and 'PAX' not in pair['symbol']:
                        s = pair['symbol'].replace('USDT', '')
                        if 'USD' not in s:
                            symbols.append(s)

        return symbols

    def post_user_listen_key(self):
        """

        Returns
        -------
        response: dict;
        """
        if self._api_name == 'api':
            channel = 'userDataStream'
        else:
            channel = 'listenKey'

        builder = UrlParamsBuilder()
        r = self._create_request("POST", f'/{self._api_name}/{self._api_version}/{channel}', builder)
        response = requests.request(r.method, r.host + r.url, headers=r.header).json()
        return response

    def put_user_listen_key(self):
        """
        Returns
        -------
        response: dict;
        """

        if self._api_name == 'api':
            channel = 'userDataStream'
        else:
            channel = 'listenKey'

        builder = UrlParamsBuilder()

        r = self._create_request("PUT", f'/{self._api_name}/{self._api_version}/{channel}', builder)
        response = requests.request(r.method, r.host + r.url, headers=r.header).json()
        return response

    def get_symbol_price_ticker(self, symbol):
        """

        Parameters
        ----------
        symbol string; ticker symbol

        Returns
        -------

        """
        builder = UrlParamsBuilder()
        builder.put_url("symbol", symbol)
        r = self._create_request("GET", f"/{self._api_name}/{self._api_version}/ticker/price", builder)

        response = requests.request(r.method, r.host + r.url, headers=r.header).json()
        return response

    def get_24hr_ticker_price_change(self, symbol):
        builder = UrlParamsBuilder()
        builder.put_url("symbol", symbol)
        r = self._create_request("GET", f"/{self._api_name}/{self._api_version}/ticker/24hr", builder)

        response = requests.request(r.method, r.host + r.url, headers=r.header).json()
        return response

    async def async_get_latest_n_candles_without_newest(self, symbol, interval, n_candles):
        print(f'Getting {n_candles} candles for {symbol}')
        builder = UrlParamsBuilder()
        builder.put_url("symbol", symbol)
        builder.put_url("interval", interval)
        builder.put_url("limit", n_candles)

        r = self._create_request("GET", f"/{self._api_name}/{self._api_version}/klines", builder)

        async with ClientSession(trust_env=True) as session:
            async with session.request(r.method, r.host + r.url, headers=r.header) as response:
                body = await response.json()

                if len(body) <= 3:
                    return []
                else:
                    cur_time = datetime.now().strftime('%Y-%m-%d %H:%M')
                    latest_candle_open_time = datetime.fromtimestamp(body[-1][0] / 1e3).strftime('%Y-%m-%d %H:%M')

                    if latest_candle_open_time == cur_time:
                        body.pop()

                    latest_candle_open_time = datetime.fromtimestamp(body[-1][0] / 1e3).strftime('%Y-%m-%d %H:%M')
                    desired_time = (datetime.now() - timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M')

                    if latest_candle_open_time == desired_time:
                        return body
                    else:
                        return []

    def run_async_get_latest_n_candles_without_newest(self, symbols, interval, n_candles):

        async def return_jobs():
            jobs = []
            for symbol in symbols:
                jobs.append(self.async_get_latest_n_candles_without_newest(*[symbol, interval, n_candles]))
            return await asyncio.gather(*jobs)

        return asyncio.run(return_jobs())

    def get_candlestick_data(self, symbol, interval, startDate, endDate):
        """

        Parameters
        ----------
        symbol string; ticker symbol
        interval string; '1m', '5m', '1h', '4h', '1d'
        startDate string; 'YYYYMMDD'
        endTime string; 'YYYYMMDD'

        Returns
        -------
        df pd.DataFrame
        """
        startTime = datetime.strptime(startDate, '%Y%m%d').replace(tzinfo=timezone.utc).timestamp() * 1e3
        end = datetime.strptime(endDate, '%Y%m%d').replace(tzinfo=timezone.utc).timestamp() * 1e3

        all_candles = []
        fetching = True
        nth_consecutive_error = 1

        while fetching:
            builder = UrlParamsBuilder()
            builder.put_url("symbol", symbol)
            builder.put_url("interval", interval)
            builder.put_url("startTime", int(startTime))
            builder.put_url("limit", 1000)

            r = self._create_request("GET", f"/{self._api_name}/{self._api_version}/klines", builder)
            response = requests.request(r.method, r.host + r.url, headers=r.header).json()

            if 'code' not in response:
                nth_consecutive_error = 1
                if len(response) == 0:
                    fetching = False
                else:
                    all_candles += response

                    if 'm' in interval:
                        startTime += 1000 * int(interval[:-1]) * 60 * 1000
                    elif 'h' in interval:
                        startTime += 1000 * int(interval[:-1]) * 60 * 60 * 1000
                    elif 'd' in interval:
                        startTime += 1000 * int(interval[:-1]) * 60 * 60 * 24 * 1000
            else:
                time.sleep(0.5)
                if nth_consecutive_error >= 5:
                    print('TRIED 5 TIMES ALREADY, STOP!')
                    fetching = False
                else:
                    print(f'SOMETHING WEIRD ON THE RESPONSE: {response}, CREATE REQUEST FOR THE AGAIN!')
                    nth_consecutive_error += 1

            if startTime > end:
                fetching = False

        df = pd.DataFrame(all_candles,
                          columns=['openTime',
                                   'open',
                                   'high',
                                   'low',
                                   'close',
                                   'volume',
                                   'closeTime',
                                   'quoteAssetVolume',
                                   'numberOfTrades',
                                   'takerBuyBaseAssetVolume',
                                   'takerBuyQuoteAssetVolume',
                                   'ignored'],
                          dtype=np.float64
                          )
        try:
            df['dateTime'] = df.openTime.apply(lambda t: datetime.utcfromtimestamp(t / 1e3))
            return df.drop('ignored', axis=1)[df['dateTime'] < endDate].drop_duplicates()
        except Exception as e:
            print(f'CANNOT CONVERT OPENTIME TO DATETIME: {e}')
            return df

    def get_order_book(self, symbol, limit):
        """

        Parameters
        ----------
        symbol string; ticker symbol
        limit int; levels of ob

        Returns
        -------
        response: json
        """
        builder = UrlParamsBuilder()
        builder.put_url("symbol", symbol)
        builder.put_url('limit', limit)

        r = self._create_request("GET", f"/{self._api_name}/{self._api_version}/depth", builder)
        response = requests.request(r.method, r.host + r.url, headers=r.header).json()
        return response

    def get_recent_trades(self, symbol, limit=1000):
        """

        Parameters
        ----------
        symbol string; ticker symbol
        limit int; number of trades

        Returns
        -------
        response: json
        """
        builder = UrlParamsBuilder()
        builder.put_url("symbol", symbol)
        builder.put_url("limit", limit)

        r = self._create_request("GET", f"/{self._api_name}/{self._api_version}/trades", builder)
        response = requests.request(r.method, r.host + r.url, headers=r.header).json()

        return response

    def get_historical_trades(self, symbol, fromId):
        """

        Parameters
        ----------
        symbol string; ticker symbol
        fromId int; trade id to fetch from

        Returns
        -------

        """
        builder = UrlParamsBuilder()
        builder.put_url("symbol", symbol)
        builder.put_url("fromId", fromId)

        r = self._create_request("GET", f"/{self._api_name}/{self._api_version}/historicalTrades", builder)
        response = requests.request(r.method, r.host + r.url, headers=r.header).json()

        return response

    def get_position_mode(self):
        """

        Returns
        -------
        response: dict;
        """
        if self._api_name == 'api':
            pass
        else:
            builder = UrlParamsBuilder()

            r = self._create_request_with_signature("GET", f"/{self._api_name}/{self._api_version}/positionSide/dual",
                                                    builder)
            response = requests.request(r.method, r.host + r.url, headers=r.header).json()

            return response

    def get_position_info(self, symbol):
        """

        Parameters
        ----------
        symbol string; ticker symbol;

        Returns
        -------
        response: dict;
        """
        if self._api_name == 'api':
            pass

        else:
            builder = UrlParamsBuilder()
            builder.put_url("symbol", symbol)

            if self._api_name == 'dapi':
                version = 'v1'
            else:
                version = 'v2'
            r = self._create_request_with_signature("GET", f"/{self._api_name}/{version}/positionRisk",
                                                    builder)
            response = requests.request(r.method, r.host + r.url, headers=r.header).json()

            return response

    def get_all_positions(self):
        """

        Parameters
        ----------
        symbol string; ticker symbol;

        Returns
        -------
        response: dict;
        """
        if self._api_name == 'api':
            pass

        else:
            builder = UrlParamsBuilder()

            if self._api_name == 'dapi':
                version = 'v1'
            else:
                version = 'v2'
            r = self._create_request_with_signature("GET", f"/{self._api_name}/{version}/positionRisk",
                                                    builder)
            response = requests.request(r.method, r.host + r.url, headers=r.header).json()

            return response

    def get_any_position(self, symbol):
        """

        Parameters
        ----------
        symbol string; ticker symbol;

        Returns
        -------
        response: dict;
        """
        if self._api_name == 'api':
            pass
        else:
            builder = UrlParamsBuilder()
            builder.put_url("symbol", symbol)

            r = self._create_request_with_signature("GET", f"/{self._api_name}/v2/positionRisk",
                                                    builder)
            response = requests.request(r.method, r.host + r.url, headers=r.header).json()

            in_position = False

            for r in response:
                if float(r['positionAmt']) != 0:
                    in_position = True

            return in_position

    def post_position_mode(self, dualSidePosition):
        """

        Parameters
        ----------
        dualSidePosition string; "true" or "false"

        Returns
        -------
        response: dict;
        """
        if self._api_name == 'api':
            pass
        else:
            builder = UrlParamsBuilder()
            builder.put_url("dualSidePosition", dualSidePosition)

            r = self._create_request_with_signature("POST", f"/{self._api_name}/{self._api_version}/positionSide/dual",
                                                    builder)
            response = requests.request(r.method, r.host + r.url, headers=r.header).json()

            return response

    def get_account_info(self):
        """

        Returns
        -------
        response: dict;
        """
        builder = UrlParamsBuilder()

        r = self._create_request_with_signature("GET", f"/{self._api_name}/{self._api_version}/account", builder)
        response = requests.request(r.method, r.host + r.url, headers=r.header).json()

        return response

    def post_change_initial_margin(self, symbol, leverage):
        """

        Parameters
        ----------
        symbol string; ticker symbol
        leverage int; leverage of

        Returns
        -------
        response: dict;
        """
        if self._api_name == 'api':
            pass
        else:
            builder = UrlParamsBuilder()
            builder.put_url("symbol", symbol)
            builder.put_url("leverage", leverage)

            r = self._create_request_with_signature("POST", f"/{self._api_name}/{self._api_version}/leverage", builder)
            response = requests.request(r.method, r.host + r.url, headers=r.header).json()

            return response

    def post_new_market_order(self, symbol, side, quantity, positionSide='BOTH', reduceOnly=False):
        """

        Parameters
        ----------
        symbol string; ticker symbol
        side string; 'buy' or 'sell'
        quantity float; qty
        positionSide string; "BOTH" for ONE-WAY mode, "LONG" or "SHORT" for HEDGED mode

        Returns
        -------
        response: dict;
        """
        builder = UrlParamsBuilder()
        builder.put_url("symbol", symbol)
        builder.put_url("side", side)
        builder.put_url("type", 'MARKET')
        builder.put_url("quantity", quantity)

        if positionSide == 'BOTH':
            builder.put_url("reduceOnly", reduceOnly)

        if self._api_name == 'fapi' or self._api_name == 'dapi':
            builder.put_url("positionSide", positionSide)

        r = self._create_request_with_signature("POST", f"/{self._api_name}/{self._api_version}/order", builder)

        response = requests.request(r.method, r.host + r.url, headers=r.header).json()
        return response

    def place_market_order(self, symbol, side, quantity, positionSide='BOTH'):
        return self.post_new_market_order(symbol, side, quantity, positionSide)

    async def async_post_new_market_order(self, symbol, side, quantity, positionSide='BOTH', reduceOnly=False):
        """
        async version of market order;
        Parameters
        ----------
        symbol string; ticker symbol
        side string; 'buy' or 'sell'
        quantity float; qty
        positionSide string; "BOTH" for ONE-WAY mode, "LONG" or "SHORT" for HEDGED mode

        Returns
        -------
        body: dict;
        """

        builder = UrlParamsBuilder()
        builder.put_url("symbol", symbol)
        builder.put_url("side", side)
        builder.put_url("type", 'MARKET')
        builder.put_url("quantity", quantity)

        if positionSide == 'BOTH':
            builder.put_url("reduceOnly", reduceOnly)

        if self._api_name == 'fapi' or self._api_name == 'dapi':
            builder.put_url("positionSide", positionSide)

        r = self._create_request_with_signature("POST", f"/{self._api_name}/{self._api_version}/order", builder)

        async with ClientSession(trust_env=True) as session:
            async with session.request(r.method, r.host + r.url, headers=r.header) as response:
                body = await response.json()
                return body

    def async_execute_post_new_market_orders(self, orders):

        async def return_jobs():
            jobs = []
            for order in orders:
                jobs.append(self.async_post_new_market_order(*order))
            return await asyncio.gather(*jobs)

        return asyncio.run(return_jobs())

    def post_new_limit_order(self, symbol, side, price, quantity, positionSide='BOTH', timeInForce='GTC',
                             reduceOnly=False):
        """

        Parameters
        ----------
        symbol string; ticker symbol
        side string; 'buy' or 'sell'
        quantity float; qty
        positionSide string; "BOTH" for ONE-WAY mode, "LONG" or "SHORT" for HEDGED mode

        Returns
        -------
        response: dict;
        """
        builder = UrlParamsBuilder()
        builder.put_url("symbol", symbol)
        builder.put_url("side", side)
        builder.put_url("type", 'LIMIT')
        builder.put_url("timeInForce", timeInForce)
        builder.put_url("quantity", quantity)
        builder.put_url("price", price)

        if positionSide == 'BOTH':
            builder.put_url("reduceOnly", reduceOnly)

        if self._api_name == 'fapi' or self._api_name == 'dapi':
            builder.put_url("positionSide", positionSide)

        r = self._create_request_with_signature("POST", f"/{self._api_name}/{self._api_version}/order", builder)

        response = requests.request(r.method, r.host + r.url, headers=r.header).json()
        return response

    async def async_post_new_limit_order(self, symbol, side, price, quantity, positionSide='BOTH', timeInForce='GTC',
                                         reduceOnly=False):
        """
        async version of limit order;
        Parameters
        ----------
        symbol string; ticker symbol
        side string; 'buy' or 'sell'
        price float; price to buy or sell
        quantity float; qty
        positionSide string; "BOTH" for ONE-WAY mode, "LONG" or "SHORT" for HEDGED mode
        timeInForce string; 'GTC', 'IOC', 'FOK', 'GTX"

        Returns
        -------
        body: dict;
        """
        builder = UrlParamsBuilder()
        builder.put_url("symbol", symbol)
        builder.put_url("side", side)
        builder.put_url("type", 'LIMIT')
        builder.put_url("timeInForce", timeInForce)
        builder.put_url("quantity", quantity)
        builder.put_url("price", price)

        if positionSide == 'BOTH':
            builder.put_url("reduceOnly", reduceOnly)

        if self._api_name == 'fapi' or self._api_name == 'dapi':
            builder.put_url("positionSide", positionSide)

        r = self._create_request_with_signature("POST", f"/{self._api_name}/{self._api_version}/order", builder)

        async with ClientSession(trust_env=True) as session:
            async with session.request(r.method, r.host + r.url, headers=r.header) as response:
                body = await response.json()
                return body

    def async_execute_post_new_limit_orders(self, orders):

        async def return_jobs():
            jobs = []
            for order in orders:
                jobs.append(self.async_post_new_limit_order(*order))
            return await asyncio.gather(*jobs)

        return asyncio.run(return_jobs())

    async def post_new_stop_limit_order(self, symbol, side, price, quantity, stopPrice, positionSide='BOTH',
                                        timeInForce='GTC'):
        """
        async version of stop limit order;
        Parameters
        ----------
        symbol string; ticker symbol
        side string; 'buy' or 'sell'
        price float; price to buy or sell
        stopPrice float; price to trigger the order
        quantity float; qty
        positionSide string; "BOTH" for ONE-WAY mode, "LONG" or "SHORT" for HEDGED mode
        timeInForce string; 'GTC', 'IOC', 'FOK', 'GTX"

        Returns
        -------
        body: dict;
        """
        builder = UrlParamsBuilder()
        builder.put_url("symbol", symbol)
        builder.put_url("side", side)
        builder.put_url("type", 'STOP')
        builder.put_url('stopPrice', stopPrice)
        builder.put_url("timeInForce", timeInForce)
        builder.put_url("quantity", quantity)
        builder.put_url("price", price)
        if self._api_name == 'fapi':
            builder.put_url("positionSide", positionSide)

        r = self._create_request_with_signature("POST", f"/{self._api_name}/{self._api_version}/order", builder)

        async with ClientSession(trust_env=True) as session:
            async with session.request(r.method, r.host + r.url, headers=r.header) as response:
                body = await response.json()
                return body

    async def post_new_stop_market_order(self, symbol, side, quantity, stopPrice, positionSide='BOTH',
                                         closePosition='false'):
        """
        async version of stop limit order;
        Parameters
        ----------
        symbol string; ticker symbol
        side string; 'buy' or 'sell'
        quantity float; qty
        stopPrice float; price to trigger the order
        positionSide string; "BOTH" for ONE-WAY mode, "LONG" or "SHORT" for HEDGED mode
        timeInForce string; 'GTC', 'IOC', 'FOK', 'GTX"
        closePosition string; 'true' or 'false'

        Returns
        -------
        body: dict;
        """
        builder = UrlParamsBuilder()
        builder.put_url("symbol", symbol)
        builder.put_url("side", side)
        builder.put_url("type", 'STOP_MARKET')
        builder.put_url("quantity", quantity)
        builder.put_url('stopPrice', stopPrice)
        builder.put_url('closePosition', closePosition)
        if self._api_name == 'fapi':
            builder.put_url("positionSide", positionSide)

        r = self._create_request_with_signature("POST", f"/{self._api_name}/{self._api_version}/order", builder)

        async with ClientSession(trust_env=True) as session:
            async with session.request(r.method, r.host + r.url, headers=r.header) as response:
                body = await response.json()
                return body

    def get_all_open_orders(self, symbol):
        """

        Parameters
        ----------
        symbol string; ticker symbol

        Returns
        -------
        response: dict;
        """
        builder = UrlParamsBuilder()
        builder.put_url("symbol", symbol)

        r = self._create_request_with_signature("GET", f"/{self._api_name}/{self._api_version}/openOrders", builder)
        response = requests.request(r.method, r.host + r.url, headers=r.header).json()

        return response

    def get_order_status(self, symbol, orderId):
        """

        Parameters
        ----------
        symbol string; ticker symbol
        orderId int; order Id

        Returns
        -------
        response: dict;
        """
        builder = UrlParamsBuilder()
        builder.put_url("symbol", symbol)
        builder.put_url("orderId", orderId)

        r = self._create_request_with_signature("GET", f"/{self._api_name}/{self._api_version}/order", builder)
        response = requests.request(r.method, r.host + r.url, headers=r.header).json()

        return response

    async def async_get_order_status(self, symbol, orderId):
        builder = UrlParamsBuilder()
        builder.put_url("symbol", symbol)
        builder.put_url("orderId", orderId)

        r = self._create_request_with_signature("GET", f"/{self._api_name}/{self._api_version}/order", builder)

        async with ClientSession(trust_env=True) as session:
            async with session.request(r.method, r.host + r.url, headers=r.header) as response:
                body = await response.json()
                return body

    def async_run_get_order_status(self, symbols, orderIds):

        async def return_jobs():
            jobs = []
            for i in range(len(symbols)):
                jobs.append(self.async_get_order_status(symbols[i], orderIds[i]))
            return await asyncio.gather(*jobs)

        return asyncio.run(return_jobs())

    def delete_a_open_order(self, symbol, orderId):
        """

        Parameters
        ----------
        symbol string; ticker symbol
        orderId int; order Id

        Returns
        -------
        response: dict;
        """
        builder = UrlParamsBuilder()
        builder.put_url("symbol", symbol)
        builder.put_url("orderId", orderId)

        r = self._create_request_with_signature("DELETE", f"/{self._api_name}/{self._api_version}/order", builder)
        response = requests.request(r.method, r.host + r.url, headers=r.header).json()

        return response

    async def delete_all_open_orders(self, symbol):
        """

        Parameters
        ----------
        symbol string; ticker symbol
        orderId int; order Id

        Returns
        -------
        body: dict;
        """
        builder = UrlParamsBuilder()
        builder.put_url("symbol", symbol)

        r = self._create_request_with_signature("DELETE", f"/{self._api_name}/{self._api_version}/allOpenOrders",
                                                builder)

        async with ClientSession(trust_env=True) as session:
            async with session.request(r.method, r.host + r.url, headers=r.header) as response:
                body = await response.json()
                return body

    def del_all_open_orders_countdown(self, symbol, countdownTime):
        """

        Parameters
        ----------
        symbol string; ticker symbol
        countdownTime int; in milliseconds;

        Returns
        -------
        response: dict;
        """
        builder = UrlParamsBuilder()
        builder.put_url("symbol", symbol)
        builder.put_url("countdownTime", countdownTime)

        r = self._create_request_with_signature("DELETE", f"/{self._api_name}/{self._api_version}/countdownCancelAll",
                                                builder)

        response = requests.request(r.method, r.host + r.url, headers=r.header).json()

        return response

    def get_account_trade_history(self, symbol):
        """

        Parameters
        ----------
        symbol string; ticker symbol

        Returns
        -------
        response: dict;
        """
        builder = UrlParamsBuilder()
        builder.put_url("symbol", symbol)

        r = self._create_request_with_signature("GET", f"/{self._api_name}/{self._api_version}/userTrades",
                                                builder)

        response = requests.request(r.method, r.host + r.url, headers=r.header).json()

        return response

    def close_all_positions(self):
        all_positions = self.get_all_positions()

        for position in all_positions:
            if float(position['positionAmt']) > 0:
                self.post_new_market_order(position['symbol'], 'SELL', float(position['positionAmt']), 'LONG')

            elif float(position['positionAmt']) < 0:
                self.post_new_market_order(position['symbol'], 'BUY', -float(position['positionAmt']), 'SHORT')

    def close_all_position_except(self, except_list):
        all_positions = self.get_all_positions()

        for position in all_positions:
            if position['symbol'] not in except_list:
                if float(position['positionAmt']) > 0:
                    self.post_new_market_order(position['symbol'], 'SELL', float(position['positionAmt']), 'LONG')

                elif float(position['positionAmt']) < 0:
                    self.post_new_market_order(position['symbol'], 'BUY', -float(position['positionAmt']), 'SHORT')