import telegram
import pymarketcap

from retry import retry
from retry.api import retry_call


TOKEN = '402107309:AAE-V2bd9KY2kyVvbY-o6F453PnxGB5mfwY'
CHAT_ID = 111827564


@retry(tries=3, delay=1)
def send_notifier(msg, mark=None, keyboard=None):
    while True:
        try:
            bot = telegram.Bot(TOKEN)
            bot.sendMessage(CHAT_ID, msg, parse_mode=mark, reply_markup=keyboard)
            break
        except Exception as e:
            err_code, err_text = e
            if not ((err_code == 104) and (err_text == 'Connection reset by peer')):
                break


def get_conversion(BASE_CURRENCIES, FIAT):
    conversion = {}
    coinmarketcap = pymarketcap.Pymarketcap()
    for base in BASE_CURRENCIES:
        if base in FIAT:
            continue
        ticker = retry_call(coinmarketcap.ticker, fargs=[base], tries=10, delay=0.5)
        price_usd = ticker['price_usd']
        conversion[base + '_' + 'USD'] = float(price_usd)

    conversion.update({'USD_USD': 1})
    conversion.update({'CNY_USD': 0.15})
    return conversion
