import sqlite3
import math


def compute_rolling_average_price_and_qty(original_average_price, original_qty, new_qty, new_price):
    original_total = original_average_price * original_qty
    new_total = new_qty * new_price

    new_average_price = (original_total + new_total) / (original_qty + new_qty)

    return new_average_price, original_qty + new_qty


def compute_precision(exchange, market, coin, client):
    if exchange.upper() == 'BINANCE':
        exchange_information = client.get_exchange_information()
        for info in exchange_information['symbols']:
            if info['symbol'] == coin:
                if 'FUTURES' in market:
                    precision = int(info['quantityPrecision'])
                    break
                elif market == 'SPOT':
                    minQty = str(float(info['filters'][2]['minQty']))
                    if minQty.split('.')[-1] == '0':
                        precision = 0
                    else:
                        precision = len(minQty.split('.')[-1])
                    break
        else:
            raise Exception(f'Cannot find coin {coin} in Binance!')

    elif exchange.upper() == 'COINBASE':
        # precision = 8
        all_products = client.get_products()

        for product in all_products:
            if product['id'] == coin.upper():
                precision = len(product['base_increment'].split('.')[1].split('1')[0]) + 1
                break
        else:
            raise Exception(f'Cannot find coin {coin} in Coinbase!')

    elif exchange.upper() == 'DERIBIT':
        precision = 0

    elif exchange.upper() == 'OKEX':
        if 'FUTURES' in market.upper():
            precision = 0
        else:
            pair_info = client.get_trading_pair_info(coin)
            precision = len(pair_info['size_increment'].split('.')[1].split('1')[0]) + 1

    return precision


spacedict = {
    1: '&#9;''&#9;''&#9;''&#9;''&#9;',
    2: '&#9;''&#9;''&#9;''&#9;',
    3: '&#9;''&#9;''&#9;',
    4: '&#9;''&#9;',
    5: '&#9;',
}
