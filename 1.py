import ccxt
import time
import pymarketcap

from retry.api import retry_call
from collections import defaultdict, namedtuple

from tools import send_notifier
from tools import get_conversion

EXCHANGES = ['bittrex', 'hitbtc', 'cryptopia', 'yobit', 'liqui',
             'bit2c', 'huobi', 'btcchina']
   #          'novaexchange', 'coinexchange']
# exchanges = ['cryptopia']
BASE_CURRENCIES = ['USD', 'USDT', 'CNY',  'BTC', 'ETH']
FIAT = ['USD', 'CNY']
VOLUME_COIN_THRESHOLD = {}
VOLUME_THRESHOLD_USD = 200

IGNORED = ['USDT', 'BTC', 'ETH', 'LTC', 'AEON', 'XDN', 'XMR', 'MGO', 'WAVES']


def get_need_volumes():
    coinmarketcap = pymarketcap.Pymarketcap()
    for ticker in coinmarketcap.ticker():
        coin = ticker['symbol']
        value = ticker['price_usd']
        price_usd = float(1e-9 if not value else float(value))
        need_volume = VOLUME_THRESHOLD_USD / price_usd
        VOLUME_COIN_THRESHOLD[coin] = need_volume


# CONVERSION = {
#     'BTC_USD': 4580,
#     # 'BTC_USDT': 4325,
#     'ETH_USD': 364,
#     # 'ETH_USDT': 335,
#     'USDT_USD': 1,
#     'USD_USD': 1,
#     'CNY_USD': 0.15,
# }


RETURN = 0.05
WORKERS_COUNT = 10
CONVERSION = {}
CLIENTS = {}

def init_logger():
    import logging
    log = logging.getLogger('runner_night')
    log.setLevel(logging.WARNING)
    # create file handler which logs even debug messages
    fh = logging.FileHandler('runner_night.log')
    fh.setLevel(logging.WARNING)
    # create console handler with a higher log level
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    # add the handlers to the logger
    log.addHandler(fh)
    return log


log = init_logger()

def get_clients():
    clients = {}
    for exch in EXCHANGES:
        try:
            exchange = getattr(ccxt, exch)()
        except:
            print('Failed to get client %s' % exch)
            continue
        clients[exch] = exchange
    return clients


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

def get_base_and_coin(currency_pair):
    cur1, cur2 = currency_pair.split('/')
    if cur1 in BASE_CURRENCIES and cur2 not in BASE_CURRENCIES:
        return cur1, cur2
    elif cur1 not in BASE_CURRENCIES and cur2 in BASE_CURRENCIES:
        return cur2, cur1

    # FIAT/BTC FIAT/ETH OR ETH/BTC
    if cur1 in ['USD', 'USDT']:
        return cur1, cur2
    elif cur2 in ['USD', 'USDT']:
        return cur2, cur1
    return None, None


def get_usd_price(base, coin):
    return CONVERSION[base + '_' + 'USD']


class Deal(namedtuple('Deal', ['price', 'quantity', 'side'])):
    def __repr__(self):
        return '\n(p={},q={},{})'.format(*self)


class MarketInfo(namedtuple('MarketInfo', ['exchange', 'volume', 'market', 'price', 'deals'])):
    def __repr__(self):
        return '-----------\n e={},v={},mkt={},p={} \n d={} \n------------\n'.format(*self)


def fetch_order_book(func, market):
    import random
    for _ in range(10):
        try:
            return func(market)
        except Exception as e:
            if 'too often' in str(e):
                wait = 0.1 + random.random()
                time.sleep(wait)
            else:
                break
    raise RuntimeError


def process_market(exch, market, need_volume):
    base, coin = get_base_and_coin(market)
    if not base:
        return [], []
    buy_prices = []
    sell_prices = []
    client = CLIENTS[exch]
    try:
        order_book = fetch_order_book(client.fetch_order_book, market)
    except Exception as e:
        print(e)
        print('Failed to fetch order_book %s %s' % (exch, market))
        return [], []
    bids, asks = order_book['bids'], order_book['asks']
    buy_spend, deals_buy = get_sum_on_volume(asks, need_volume, 'BUY')
    sell_spend, deals_sell = get_sum_on_volume(bids, need_volume, 'SELL')
    buy_sum_usd = buy_spend * get_usd_price(base, coin)
    sell_sum_usd = sell_spend * get_usd_price(base, coin)

    if buy_sum_usd:
        buy_prices.append(MarketInfo(exchange=exch, market=market, price=buy_sum_usd,
                                     volume=need_volume, deals=deals_buy))
    if sell_sum_usd:
        sell_prices.append(MarketInfo(exchange=exch, market=market, price=sell_sum_usd,
                                      volume=need_volume, deals=deals_sell))
    return buy_prices, sell_prices
    # prices[coin]['buys'] = buy_prices
    # prices[coin]['sells'] = sell_prices
    # buy_prices.sort()
    # sell_prices.sort(reverse=True)

from pymarketcap.up import get_currencies
from pymarketcap import Pymarketcap


def process_coins(worker_id, coins_markets):
    global CONVERSION
    # global VOLUME_COIN_THRESHOLD
    while True:
        log.warning('Worker #%s processing coins' % worker_id)
        CONVERSION = get_conversion(BASE_CURRENCIES, FIAT)
        get_need_volumes()
        for coin, markets in coins_markets:
            process_coin(coin, markets)


def process_coin(coin, markets):
    # print(coin_markets)
    # coin, markets = coin_markets
    if coin in IGNORED:
        return
    print('Processing coin %s' % coin)
    buys = []
    sells = []
    # prices = defaultdict(lambda: defaultdict(lambda: []))
    for market in markets:
        exchange = market['exchange']
        pair = market['pair']
        if exchange not in CLIENTS.keys():
            continue
        print('Processing %s' % market)
        buy_pr, sell_pr = process_market(exchange, pair, VOLUME_COIN_THRESHOLD[coin])

        buys += buy_pr
        sells += sell_pr
    buys.sort(key=lambda v: v.price)
    sells.sort(key=lambda v: v.price, reverse=True)

    if not buys or not sells:
        return

    best_bid = buys[0].price
    best_ask = sells[0].price
    if coin == 'MYB':
        log.warning('MYB %s %s' % (best_bid, best_ask))
    # print(buys, sells)
    for bid in buys:
        for ask in sells:
            bid_price = bid.price
            ask_price = ask.price
            if bid_price * (1 + RETURN) < ask_price:
                roi = best_ask / best_bid - 1
                #print(roi, coin, buys[0], buys[1], sells[0])
                exch_fr, exch_to = bid.exchange, ask.exchange
                mkt_fr, mkt_to = bid.market, ask.market
                log.warning('coin=%s ROI=%.1f from=%s to=%s mkt_from=%s mkt_to=%s' % (coin, roi * 100,
                                                                                      exch_fr, exch_to,
                                                                                      mkt_fr, mkt_to))
                log.warning('%s %s' % (bid.deals, ask.deals))


from multiprocessing import Process


def get_markets(coin, volume_threshold):
    cm = Pymarketcap()
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

def main():
    global CONVERSION
    global CLIENTS
    # data = Pymarketcap()._up()
    # all_markets = {coin_data['symbol']: get_markets(coin_data['symbol'], 2000)
    #                for coin_data in data}
    import json

    with open('all_markets.json', 'r') as fd:
        data = fd.read()
        all_markets = json.loads(data)
        print(all_markets.items())
        # all_markets = json.loads(fd.read())
    # print('Calculating conversion')
    # CONVERSION = get_conversion(BASE_CURRENCIES, FIAT)

    print('Calculating volumes..')
    get_need_volumes()
    # print(VOLUME_COIN_THRESHOLD)
    # return
    # print(all_markets)
    print('Getting clients..')
    CLIENTS = get_clients()
    # for coin_data in data:
    #     value = coin_data['24h_volume_usd']

    #     if volume > 3000:
    #         all_coins.append(coin_data['symbol'])
    # for coin in all_coins:
    #     markets = Pymarketcap().markets(coin)
    #     print(markets)

    # return
    # all_coins = [coin_data['symbol'] for coin_data in data if

    # process_coin(list(all_markets.items())[0])
    # process_coin(('ADX', all_markets['ADX']))
    processes = []
    all_items = list(all_markets.items())
    items_size = len(all_items) // WORKERS_COUNT
    iterator = 0
    print('Starting workers')
    for worker_id in range(WORKERS_COUNT):
        if worker_id == WORKERS_COUNT - 1:
            chunk = all_items[iterator:]
        else:
            chunk = all_items[iterator:iterator + items_size]
        iterator += items_size
        proc = Process(target=process_coins, args=(worker_id, chunk))
        proc.start()
        processes.append(proc)
    print('Started workers')
    for proc in processes:
        proc.join()

    # while 1:
    #     p.map(process_coin, all_markets.items())
    #     print('Sleeping for 10 sec')
    #     time.sleep(10)
    #     print('Calculating conversion')
    #     CONVERSION = get_conversion(BASE_CURRENCIES, FIAT)
    #     print('Getting clients..')
    #     CLIENTS = get_clients()


if __name__ == "__main__":
    main()
