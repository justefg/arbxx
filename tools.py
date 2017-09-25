import telegram
import pymarketcap
import ccxt
import logging
import json

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


def get_config(filename):
    with open(filename, 'r') as fd:
        return json.loads(fd.read())


def get_sum_on_volume(data, volume, side):
    need_volume = volume
    sum_spent = 0
    deals = []
    for [price, volume] in data:
        qty = min(need_volume, volume)
        sum_spent += price * qty
        need_volume -= qty
        deals.append(Deal(price, qty, side))
        if need_volume <= 0:
            return sum_spent, deals
    return 0, deals


def get_base_and_coin(currency_pair, base_currencies):
    cur1, cur2 = currency_pair.split('/')
    if cur1 in base_currencies and cur2 not in base_currencies:
        return cur1, cur2
    elif cur1 not in base_currencies and cur2 in base_currencies:
        return cur2, cur1

    # FIAT/BTC FIAT/ETH OR ETH/BTC
    if cur1 in ['USD', 'USDT']:
        return cur1, cur2
    elif cur2 in ['USD', 'USDT']:
        return cur2, cur1
    return None, None


def get_usd_price(base, conversion):
    return conversion[base + '_' + 'USD']


def get_arb_amount(buy_prices, sell_prices):
    i = 0
    j = 0
    amount = 0
    buy_left = 0
    sell_left = 0
    while i < len(buy_prices) and j < len(sell_prices):
        buy_price, amo_buy = buy_prices[i]
        if buy_left:
            amo_buy = buy_left
        sell_price, amo_sell = sell_prices[j]
        if sell_left:
            amo_sell = sell_left
        if buy_price < sell_price:
            if amo_buy > amo_sell:
                buy_left = amo_buy - amo_sell
                amount += amo_sell
                sell_left = 0

                j += 1
            else:
                sell_left = amo_sell - amo_buy
                amount += amo_buy
                buy_left = 0

                i += 1
        else:
            break

    if sell_left:
        # We moved buy price
        sell_amount = amount + sell_left
        last_buy_price, _ = buy_prices[i - 1]
        j += 1
        while j < len(sell_prices) and sell_prices[j][0] > last_buy_price:
            sell_amount += sell_prices[j][1]
            j += 1
        return amount, sell_amount
    if buy_left:
        # We moved sell price
        buy_amount = amount + buy_left
        last_sell_price, _ = sell_prices[j - 1]
        i += 1
        while i < len(buy_prices) and buy_prices[i][0] < last_sell_price:
            buy_amount += buy_prices[i][1]
            i += 1
        return buy_amount, amount

    return amount, amount
