import requests


#
# tokens = {'pgex_binance_bot': '1492399420:AAGz-JOY0mBijyHJdDlzLjlAKFU_Yevmq0c',
#           'pgex_twap_bot': '1295945455:AAGXZyYlio6JSbvHgsV4M4oaqZg-s_K0NKQ',
#           'pgex_loop_bot': '1457551307:AAG5l-LfGsFdkJ-rUXmuxh84WhkkGResPOo',
#           'pgex_testing_twap_bot': '1489317853:AAGvp1abPwU5cb8YHCdnBf9hJKWXqe4sAgM',
#           'pgex_testing_loop_bot': '1550956715:AAFIIBxjFkUZCt3QwwFKdrQE_YIunsP80m4'
#           }
#
# chat_id = {'angus': [982076204],
#            'alex': [720383934],
#            'twap_group': [-451924029],
#            'twap_test_group': [-401704918]}


class TgBotAPI:
    def __init__(self, token):
        self._token = token

    def send_message(self, chat_id, message, parse_mode):
        url = f'https://api.telegram.org/bot{self._token}/sendMessage'
        data = {'chat_id': chat_id, 'text': message, 'parse_mode': parse_mode}
        return requests.post(url, data).json()

    def edit_message(self, chat_id, message_id, message, parse_mode):
        url = f'https://api.telegram.org/bot{self._token}/editMessageText'
        data = {'chat_id': chat_id, 'message_id': message_id, 'text': message, 'parse_mode': parse_mode}
        return requests.post(url, data).json()

    def get_updates(self, offset=0):
        url = f'https://api.telegram.org/bot{self._token}/getUpdates?offset={offset}'
        return requests.post(url).json()
