#           'pgex_testing_twap_bot': '1489317853:AAGvp1abPwU5cb8YHCdnBf9hJKWXqe4sAgM',
#           'pgex_testing_loop_bot': '1550956715:AAFIIBxjFkUZCt3QwwFKdrQE_YIunsP80m4'
from telegram import ParseMode
from twapExecution.tgBot.tgBotAPI import TgBotAPI
bot1 = TgBotAPI(token='1489317853:AAGvp1abPwU5cb8YHCdnBf9hJKWXqe4sAgM')
bot2 = TgBotAPI(token='1550956715:AAFIIBxjFkUZCt3QwwFKdrQE_YIunsP80m4')

bot1.send_message(chat_id=-401704918, message='你好呀~', parse_mode=ParseMode.HTML)

bot2.get_updates()