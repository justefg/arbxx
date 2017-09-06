import telegram
import pymarketcap
import ccxt
import logging

from retry import retry
from retry.api import retry_call


@retry(tries=5, delay=1)
def send_notifier(chat_id, token, msg, mark=None, keyboard=None):
    while True:
        try:
            bot = telegram.Bot(token)
            bot.sendMessage(chat_id, msg, parse_mode=mark, reply_markup=keyboard)
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


def get_need_volumes(volume_threshold_usd):
    coinmarketcap = pymarketcap.Pymarketcap()
    volume_coin_threshold = {}
    tickers = retry_call(coinmarketcap.ticker, tries=10, delay=0.5)
    for ticker in tickers:
        coin = ticker['symbol']
        value = ticker['price_usd']
        price_usd = float(1e-9 if not value else float(value))
        need_volume = volume_threshold_usd / price_usd
        volume_coin_threshold[coin] = need_volume
    return volume_coin_threshold


def get_clients(exchanges):
    clients = {}
    for exch in exchanges:
        try:
            attr = getattr(ccxt, exch)
            exchange = retry_call(attr, tries=10, delay=0.2)
            # exchange = getattr(ccxt, exch)()
        except:
            print('Failed to get client %s' % exch)
            continue
        clients[exch] = exchange
    return clients


def get_logger(filename):
    log = logging.getLogger('runner_night')
    log.setLevel(logging.WARNING)
    # create file handler which logs even debug messages
    fh = logging.FileHandler(filename)
    fh.setLevel(logging.WARNING)
    # create console handler with a higher log level
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    # add the handlers to the logger
    log.addHandler(fh)
    return log



def get_markets(coin, volume_threshold):
    cm = pymarketcap.Pymarketcap()
    print('Processing %s' % coin)
    try:
        cm_markets = cm.markets(coin)
    except:
        print('Failed to process')
        return []
    result = []
    for market in cm_markets:
        value = market['24h_volume_usd']
        volume = float(0 if value is None else value)
        if volume > volume_threshold:
            result.append({
                'pair': market['pair'],
                'exchange': market['source'].lower()
            })
    return result
