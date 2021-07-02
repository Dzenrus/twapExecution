import os
import subprocess
import threading
import time

from cbpro import AuthenticatedClient
from telegram import ParseMode
from telegram.ext import CommandHandler
from telegram.ext import Updater

from twapExecution.exchanges.binance.binanceClient import BinanceClient
from twapExecution.exchanges.deribit.deribitClient import DeribitClient
from twapExecution.exchanges.env import env_vars
from twapExecution.exchanges.huobi.huobiSpotClient import HuobiSpotClient
from twapExecution.exchanges.okex.okexFuturesClient import OkexFuturesClient
from twapExecution.exchanges.okex.okexSpotClient import OkexSpotClient
from twapExecution.exchanges.utils.utils import spacedict

print(f'Telegram Start --- {threading.enumerate()}')

updater = Updater(token=env_vars['TELEGRAM_TWAP_BOT'],
                  use_context=True)

dispatcher = updater.dispatcher
print(f'Telegram Middle --- {threading.enumerate()}')


# START COMMAND
def start(update, context):
    exchange = context.args[0]
    market = context.args[1]
    coin = context.args[2]

    if exchange.upper() == 'DERIBIT':
        account = context.args[-1].upper()
    else:
        account = ''

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text='<code>' + f"--------------------------------\nStarting TWAP on {exchange.capitalize()} "
                                             f"{market.capitalize()} {coin.upper()}!\n--------------------------------\n"
                                             f"{' '.join(context.args)}" + '</code>',
                             parse_mode=ParseMode.HTML)

    print(f'START! --- {threading.enumerate()}')
    time.sleep(5)
    exchange = context.args[0]
    market = context.args[1]

    print('RUN TWAP SCRIPT!!')
    subprocess.call(f'nohup {os.path.join(os.getcwd(), ".env", "bin", "python")} '
                    f'-u -m twapExecution.exchanges.executionMethods.baseTWAP --args {" ".join(context.args)} > '
                    f'{os.path.join(os.getcwd(), f"{exchange.upper()}_{market.upper()}_{coin.upper()}_TWAP_{account}.log")} &',
                    shell=True)

    time.sleep(5)
    print(f'END! --- {threading.enumerate()}')


# ACCOUNT INFO COMMAND
def account(update, context):
    exchange = context.args[0].upper()
    market = context.args[1].upper()

    output_string = '<code>--------------------------------</code>\n'
    output_string += f'<code>{exchange.capitalize()} {market.capitalize()} Account Summary:\n--------------------------------</code>\n'

    if exchange == 'COINBASE':
        if market == 'SPOT':
            client = AuthenticatedClient(env_vars['COINBASE_API_KEY'],
                                         env_vars['COINBASE_SECRET_KEY'],
                                         env_vars['COINBASE_PW'],
                                         env_vars['COINBASE_SPOT_URL'])
            results = client.get_accounts()

            for info in results:
                if (float(info['balance'])) > 0.0001:
                    output_string += '<code>' + info['currency'] + spacedict[len(info['currency'])] + '|' + spacedict[
                        4] + str(round(float(info['balance']), 5)) + '</code>\n'

    elif exchange == 'BINANCE':

        if 'FUTURES' in market:
            _url = env_vars['BINANCE_USDT_FUTURES_URL'] if 'USDT' in market else env_vars['BINANCE_COIN_FUTURES_URL']

            client = BinanceClient(env_vars['BINANCE_API_KEY'],
                                   env_vars['BINANCE_SECRET_KEY'],
                                   _url)

            assets = client.get_account_info()['assets']
            for asset in assets:
                if float(asset['walletBalance']) > 0:
                    output_string += '<code>' + 'Balance:' + spacedict[
                        5] + str(
                        round(float(asset["walletBalance"]), 4)) + f' {asset["asset"]}' + '\n' + 'PNL:' + spacedict[
                                         1] + str(
                        round(float(asset["unrealizedProfit"]), 4)) + f' {asset["asset"]}' + '</code>\n'

            for position in client.get_account_info()['positions']:
                key = 'positionAmt' if 'USDT' in market else 'notionalValue'

                if float(position[key]) != 0:
                    symbol = position['symbol']
                    position_info = client.get_position_info(symbol=symbol)

                    for info in position_info:
                        if float(info['positionAmt']) != 0:
                            output_string += '<code>Product:' + spacedict[5] + info['symbol'] + '\nQty:' + \
                                             spacedict[1] + info[
                                                 'positionAmt'] + '\nPrice:' + spacedict[3] + info['entryPrice'] + \
                                             spacedict[3] + '\nLiq:' + spacedict[1] + str(
                                float(info['liquidationPrice'])) + '\nSide:' + spacedict[2] + str(
                                info['positionSide']) + '\n--------------------------------\n</code>'

        elif market == 'SPOT':
            client = BinanceClient(env_vars['BINANCE_API_KEY'],
                                   env_vars['BINANCE_SECRET_KEY'],
                                   env_vars['BINANCE_SPOT_URL'])
            assets = client.get_account_info()['balances']

            for asset in assets:
                # print(asset)
                if float(asset['free']) > 0.0001 or float(asset['locked']) > 0.0001:
                    output_string += '<code>' + asset['asset'] + spacedict[len(asset['asset'])] + '|' + spacedict[
                        4] + str(round(float(asset['free']) + float(asset['locked']), 5)) + '</code>\n'

    elif exchange == 'OKEX':

        if 'SPOT' in market:
            client = OkexSpotClient(env_vars['OKEX_API_KEY'],
                                    env_vars['OKEX_SECRET_KEY'],
                                    env_vars['OKEX_PASSPHRASE'])
            balances = client.get_account_info()

            for balance in balances:
                print(balance)
                if balance["currency"] == 'USDT':
                    if float(balance['available']) > 1:
                        output_string += '<code>' + balance['currency'] + spacedict[len(balance['currency'])] + '|' + \
                                         spacedict[
                                             4] + str(
                            round(float(balance['available']) + float(balance['hold']), 5)) + '</code>\n'
                elif float(balance['available']) > 0.001:
                    coin_price = float(client.get_symbol_price_ticker(f'{balance["currency"]}-USDT')['last'])
                    if (float(balance['available']) * coin_price) > 10:
                        output_string += '<code>' + balance['currency'] + spacedict[len(balance['currency'])] + '|' + \
                                         spacedict[
                                             4] + str(
                            round(float(balance['available']) + float(balance['hold']), 5)) + '</code>\n'

        elif 'FUTURES' in market:
            client = OkexFuturesClient(env_vars['OKEX_API_KEY'],
                                       env_vars['OKEX_SECRET_KEY'],
                                       env_vars['OKEX_PASSPHRASE'])
            assets = client.get_account_info()['info']

            for asset in assets:
                if float(assets[asset]['equity']) > 0:
                    output_string += '<code>' + 'Account:' + spacedict[
                        5] + str(assets[asset]["underlying"]) + '</code>\n'
                    output_string += '<code>' + 'Balance:' + spacedict[
                        5] + str(
                        round(float(assets[asset]["equity"]),
                              4)) + f' {assets[asset]["currency"]}\n' + '</code>\n'

            position_information = client.get_open_positions()
            try:
                for position in position_information['holding'][0]:
                    if float(position['long_qty']) != 0:
                        output_string += '<code>Product:' + spacedict[5] + position['instrument_id'] + '\nQty:' + \
                                         spacedict[1] + position[
                                             'long_qty'] + '\nPrice:' + spacedict[3] + position['long_avg_cost'] + \
                                         spacedict[3] + '\nLiq:' + spacedict[1] + str(
                            float(position['liquidation_price'])) + '\nPNL:' + spacedict[1] + str(
                            float(position['long_unrealised_pnl'])) + ' ' + position['instrument_id'].split('-')[
                                             0] + '\nSide:' + spacedict[
                                             2] + 'LONG' + '\n--------------------------------\n</code>'
                    if float(position['short_qty']) != 0:
                        output_string += '<code>Product:' + spacedict[5] + position['instrument_id'] + '\nQty:' + \
                                         spacedict[1] + position[
                                             'short_qty'] + '\nPrice:' + spacedict[3] + position['short_avg_cost'] + \
                                         spacedict[3] + '\nLiq:' + spacedict[1] + str(
                            float(position['liquidation_price'])) + '\nPNL:' + spacedict[1] + str(
                            float(position['short_unrealised_pnl'])) + ' ' + position['instrument_id'].split('-')[
                                             0] + '\nSide:' + spacedict[
                                             2] + 'SHORT' + '\n--------------------------------\n</code>'
            except  Exception as e:
                print('ERROR: ', e)

    elif exchange == 'HUOBI':

        if market == 'SPOT':
            client = HuobiSpotClient(env_vars['HUOBI_API_KEY'],
                                     env_vars['HUOBI_SECRET_KEY'])
            account_id = client.get_accounts()['data'][0]['id']
            balances = client.get_balance(id=account_id)['data']['list']

            for coin in balances:
                if coin['balance'] > '0.0001' and coin['type'] == 'trade':
                    output_string += '<code>' + coin['currency'].upper() + spacedict[len(coin['currency'])] + '|' + \
                                     spacedict[
                                         4] + str(round(float(coin['balance']), 5)) + '</code>'

    elif exchange == 'DERIBIT':

        if market == 'FUTURES':
            for account in ['MAIN', 'SUB1', 'SUB2']:
                client = DeribitClient(env_vars[f'DERIBIT_API_KEY_{account}'],
                                       env_vars[f'DERIBIT_SECRET_KEY_{account}'],
                                       env_vars['DERIBIT_WS_URL'])
                positions = client.get_positions('BTC')['result']
                equity_value = client.account_summary('BTC')['result']['equity']
                output_string += '<code>Account:' + spacedict[5] + account + '\nEquity:' + spacedict[4] + str(
                    equity_value) + '</code>'
                for position in positions:
                    if position['size'] != 0:
                        output_string += '<code>\nProduct:' + spacedict[5] + position['instrument_name'] + '\nAmount:' + \
                                         spacedict[4] + str(position['size']) + '\nQty:' + \
                                         spacedict[1] + str(position['size'] / 10) + '\nPrice:' + spacedict[3] + str(
                            position['average_price']) + \
                                         '\nPNL:' + spacedict[1] + str(position['total_profit_loss']) + \
                                         '\nLiq:' + spacedict[1] + str(
                            position['estimated_liquidation_price']) + '</code>'
                output_string += '<code>\n----------------------\n</code>'

    else:
        raise Exception('Only Coinbase/Binance/Okex/Huobi/Deribit is supported.')i

    context.bot.send_message(chat_id=update.effective_chat.id, text=output_string, parse_mode=ParseMode.HTML)


# HELP COMMAND
def help(update, context):
    instruments = '<pre>' \
                  'market      binance  coinbase  huobi  okex  deribit\n' \
                  'spot           *        *        *      *\n' \
                  'futures                                        *\n' \
                  'coin-futures   *                        *\n' \
                  'usdt-futures   *                        *\n' \
                  '</pre>'

    output_summary = '<pre>/account [exchange|market]</pre>\n'

    alert_summary = '<pre>/alert [pair|price threshold]     btcusdt; 50000</pre>\n'

    template_summary = '<pre>' \
                       '/template       templates for execution' \
                       '</pre>\n'

    output_start = '<pre>' \
                   '/start\n' \
                   '  [exchange]\n' \
                   '  [market]\n' \
                   '  [pair]        btc-usd, btc-perp, btc-yymmdd\n' \
                   '  [qty]         quantity; futures (10 and 100 for btc)\n' \
                   '  [side]        buy/sell/long-buy/long-sell/short-buy/short-sell\n' \
                   '  [limit]       price limit (must be better or worse for execution)\n' \
                   '  [mins]        execution period in minutes\n' \
                   '  [trades/min]  number of trades per minute\n' \
                   '  [cont]        continues previous execution; true*\n' \
                   '  [leverage]    futures only: 1*, 2... 125\n' \
                   '  [account]     deribit only: main, sub1*, sub2</pre>\n'

    output_stop = '<pre>/stop [exchange|market]</pre>'

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=instruments + '\n' + output_summary + '\n' + template_summary + '\n' + alert_summary + '\n' + output_start + '\n' + output_stop,
                             parse_mode=ParseMode.HTML)

    # HELP COMMAND

def alert(update, context):
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text='<pre>Hi Angus</pre>',
                             parse_mode=ParseMode.HTML)

def template(update, context):
    templates = '<pre>' \
                '(c) quantity by contract\n' \
                '(t) quantity by token\n\n' \
                'binance:\n' \
                '  spot         (t):  /start binance spot *-btc 100 50000 buy 5 30 false\n' \
                '  coin-futures (c):  /start binance coin-futures *-perp 100 50000 long-buy 5 30 false 1 \n' \
                '  usdt-futures (t):  /start binance usdt-futures *-perp 100 50000 long-buy 5 30 false 1 \n' \
                '\n' \
                'coinbase:\n' \
                '  spot         (t):  /start coinbase spot *-btc 100 50000 buy 5 30 false\n' \
                '\n' \
                'deribit:\n' \
                '  futures      (c):  /start deribit futures *-perp 100 50000 buy 5 30 false 1 sub1\n' \
                '\n' \
                'okex:\n' \
                '  spot         (t):  /start okex spot *-btc 100 50000 buy 5 30 false\n' \
                '  coin-futures (c):  /start okex coin-futures *-perp 100 50000 long-buy 5 30 false 1 \n' \
                '  usdt-futures (c):  /start okex usdt-futures *-perp 100 50000 long-buy 5 30 false 1 \n' \
                '</pre>'

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=templates,
                             parse_mode=ParseMode.HTML)


def alert(update, context):
    coin = context.args[0]
    price_threshold = context.args[1]
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text='<code>' + f"--------------------------------\nSetting Alert for "
                                             f"{coin.upper()} @ {price_threshold}!\n--------------------------------" + '</code>',
                             parse_mode=ParseMode.HTML)

    print(f'START! --- {threading.enumerate()}')
    time.sleep(5)

    print('RUN ALERT SCRIPT!!')
    subprocess.call(f'nohup {os.path.join(os.getcwd(), ".env", "bin", "python3.7")} '
                    f'-u -m twapExecution.exchanges.utils.alertBot --args {" ".join(context.args)} > '
                    f'{os.path.join(os.getcwd(), f"_{coin.upper()}_ALERT.log")} &',
                    shell=True)

    time.sleep(5)
    print(f'END! --- {threading.enumerate()}')


# PUT ALL COMMAND INTO HANDLER;
start_handler = CommandHandler('start', start, pass_args=True)
account_handler = CommandHandler('account', account)
help_handler = CommandHandler('help', help)
template_handler = CommandHandler('template', template)
alert_handler = CommandHandler('alert', alert)

print(f'Telegram After Setting Handler --- {threading.enumerate()}')

# ADD THE HANDLERS TO DISPATCHER
dispatcher.add_handler(start_handler)
dispatcher.add_handler(help_handler)
dispatcher.add_handler(template_handler)
dispatcher.add_handler(account_handler)
dispatcher.add_handler(alert_handler)

print(f'Telegram After Adding Handlers --- {threading.enumerate()}')

# START
updater.start_polling()
print(f'Telegram After Polling Started --- {threading.enumerate()}')

updater.idle()
