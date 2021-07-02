import json
from pathlib import Path
import os

my_path = Path(__file__).parent
path = os.path.join(my_path, "config.json")
print(path)
# ***************************************************************************************** #

# SET THE DEV ENVIRONMENT HERE

DEV_ENVIRONMENT = False

# ***************************************************************************************** #

with open(path) as f:
    environment = 'DEVELOPMENT' if DEV_ENVIRONMENT else 'PRODUCTION'
    print(f"---------- {'DEVELOPMENT' if DEV_ENVIRONMENT else 'PRODUCTION'} ENVIRONMENT ----------")
    env_vars = json.loads(f.read())
    env_vars = env_vars[environment]

    if not DEV_ENVIRONMENT:
        env_vars['BINANCE_API_KEY'] = os.getenv('BINANCE_API_KEY')
        env_vars['BINANCE_SECRET_KEY'] = os.getenv('BINANCE_SECRET_KEY')
        env_vars['COINBASE_API_KEY'] = os.getenv('COINBASE_API_KEY')
        env_vars['COINBASE_SECRET_KEY'] = os.getenv('COINBASE_SECRET_KEY')
        env_vars['COINBASE_PW'] = os.getenv('COINBASE_PW')
        env_vars['HUOBI_API_KEY'] = os.getenv('HUOBI_API_KEY')
        env_vars['HUOBI_SECRET_KEY'] = os.getenv('HUOBI_SECRET_KEY')
        env_vars['OKEX_API_KEY'] = os.getenv('OKEX_API_KEY')
        env_vars['OKEX_SECRET_KEY'] = os.getenv('OKEX_SECRET_KEY')
        env_vars['OKEX_PASSPHRASE'] = os.getenv('OKEX_PASSPHRASE')
        env_vars['DERIBIT_API_KEY_MAIN'] = os.getenv('DERIBIT_API_KEY_MAIN')
        env_vars['DERIBIT_SECRET_KEY_MAIN'] = os.getenv('DERIBIT_SECRET_KEY_MAIN')
        env_vars['DERIBIT_API_KEY_SUB1'] = os.getenv('DERIBIT_API_KEY_SUB1')
        env_vars['DERIBIT_SECRET_KEY_SUB1'] = os.getenv('DERIBIT_SECRET_KEY_SUB1')
        env_vars['DERIBIT_API_KEY_SUB2'] = os.getenv('DERIBIT_API_KEY_SUB2')
        env_vars['DERIBIT_SECRET_KEY_SUB2'] = os.getenv('DERIBIT_SECRET_KEY_SUB2')