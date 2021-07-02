import base64
import datetime
import hashlib
import hmac
import json
import urllib
import urllib.parse
from datetime import datetime
from urllib import parse

import requests


def generate_huobi_signature(api_key, secret_key, method, url, builder):
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    builder.put_url("AccessKeyId", api_key)
    builder.put_url("SignatureVersion", "2")
    builder.put_url("SignatureMethod", "HmacSHA256")
    builder.put_url("Timestamp", timestamp)

    host = urllib.parse.urlparse(url).hostname
    path = urllib.parse.urlparse(url).path

    # 对参数进行排序:
    keys = sorted(builder.param_map.keys())
    # 加入&
    qs0 = '&'.join(['%s=%s' % (key, parse.quote(builder.param_map[key], safe='')) for key in keys])
    # 请求方法，域名，路径，参数 后加入`\n`
    payload0 = '%s\n%s\n%s\n%s' % (method, host, path, qs0)
    dig = hmac.new(secret_key.encode('utf-8'), msg=payload0.encode('utf-8'), digestmod=hashlib.sha256).digest()
    # 进行base64编码
    s = base64.b64encode(dig).decode()
    builder.put_url("Signature", s)


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
            elif isinstance(value, dict):
                self.param_map[name] = value
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
        # self.header.update({"client_SDK_Version": "binance_futures-1.0.1-py3.7"})


class HuobiSpotClient:
    def __init__(self, api_key, secret_key, server_url='https://api.huobi.pro'):
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
        request.header.update({'Content-Type': 'application/json'})
        request.url = url + '?' + builder.build_url()

        return request

    def _create_request_with_signature(self, mode, url, builder):
        request = RestApiRequest()
        request.method = mode.upper()

        request.host = self._server_url
        # builder.put_url("recvWindow", 5000)
        generate_huobi_signature(self._api_key,
                                 self._secret_key,
                                 request.method,
                                 request.host + url,
                                 builder)
        request.header.update({'Content-Type': 'application/json'})
        # request.post_body = builder.post_map

        if request.method == 'POST':
            request.post_body = builder.post_map

        request.url = url + '?' + builder.build_url()

        return request

    def get_market_status(self):
        """

        Returns
        -------
        response: dict;
        """

        builder = UrlParamsBuilder()

        r = self._create_request("GET", f"/v2/market-status", builder)
        response = requests.request(r.method, r.host + r.url, headers=r.header).json()

        return response

    def get_accounts(self):
        builder = UrlParamsBuilder()

        r = self._create_request_with_signature("GET", f"/v1/account/accounts", builder)
        print(r.method, r.host + r.url, r.header)
        response = requests.request(r.method, r.host + r.url, headers=r.header).json()

        return response

    def get_balance(self, id):
        builder = UrlParamsBuilder()

        r = self._create_request_with_signature("GET", f"/v1/account/accounts/{id}/balance", builder)
        print(r.method, r.host + r.url, r.header)
        response = requests.request(r.method, r.host + r.url, headers=r.header).json()

        return response

    def get_symbols(self):
        builder = UrlParamsBuilder()

        r = self._create_request("GET", f"/v1/common/symbols", builder)
        response = requests.request(r.method, r.host + r.url, headers=r.header).json()

        return response

    def get_last_trades(self, symbol):
        builder = UrlParamsBuilder()
        builder.put_url("symbol", symbol.lower())

        r = self._create_request("GET", f"/market/trade", builder)
        response = requests.request(r.method, r.host + r.url, headers=r.header).json()

        return response

    def get_account_history(self, id):
        builder = UrlParamsBuilder()
        builder.put_url("account-id", id)

        r = self._create_request_with_signature("GET", f"/v1/account/history", builder)
        print(r.method, r.host + r.url, r.header)
        response = requests.request(r.method, r.host + r.url, headers=r.header).json()
        return response

    def get_ticker_summary(self, symbol):
        builder = UrlParamsBuilder()
        builder.put_url('symbol', symbol)

        r = self._create_request("GET", f"/market/detail", builder)
        print(r.method, r.host + r.url, r.header)
        response = requests.request(r.method, r.host + r.url, headers=r.header).json()
        return response

    def post_new_market_order(self, id, symbol, side, quantity):
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
        builder.put_post("account-id", id)

        if side.lower() == 'buy':
            builder.put_post("amount", round(quantity*float(self.get_ticker_summary(symbol)['tick']['close']), 2))
        elif side.lower() == 'sell':
            builder.put_post("amount", quantity)
        builder.put_post("symbol", symbol.lower())
        builder.put_post("type", side.lower() + '-market')
        r = self._create_request_with_signature("POST", f"/v1/order/orders/place", builder)
        print(r.method, r.host + r.url, r.header)
        response = requests.request(r.method,  r.host + r.url, headers=r.header, data=json.dumps(r.post_body)).json()
        return response


if __name__ == '__main__':
    huobi = HuobiSpotClient(api_key='rbr45t6yr4-19c99fd7-e30f2f5b-7f2ea', secret_key='5584fde7-3236aa08-4f6e677d-cd5b8')
    huobi.get_market_status()
    huobi.get_symbols()
    id = huobi.get_accounts()['data'][0]['id']
    for coin in huobi.get_balance(id)['data']['list']:
        if coin['balance'] != '0':
            print(coin)

    huobi.get_account_history(id)

    huobi.post_new_market_order(id=id, symbol='linausdt', side='buy', quantity=225)
    huobi.post_new_market_order(id=id, symbol='linausdt', side='sell', quantity=351)


