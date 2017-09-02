import logging
import time
from tools import get_clients, get_conversion, get_need_volumes
from structs import MarketInfo, Deal

from tools import get_logger

log = get_logger('runner_v2.log')


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


def process_coins(worker_id, coins_markets, config):
    print('1')
    while True:
        log.warning('Worker #%s processing coins' % worker_id)
        print('2')
        conversion = get_conversion(config['base_currencies'], config['fiat'])
        print('3')
        need_volumes = get_need_volumes(config['volume_threshold_usd'])
        print('4')
        clients = get_clients(config['exchanges'])
        print('5')
        for coin, markets in coins_markets:
            process_coin(coin, markets, clients,
                         conversion, need_volumes, config)


def process_coin(coin, markets,
                 clients, conversion, need_volumes,
                 config):
    print(coin, markets)
    # coin, markets = coin_markets
    if coin in config['ignored']:
        return
    print('Processing coin %s' % coin)
    buys = []
    sells = []
    # prices = defaultdict(lambda: defaultdict(lambda: []))
    for market in markets:
        exchange = market['exchange']
        pair = market['pair']
        if exchange not in clients.keys():
            continue
        print('Processing %s' % market)
        buy_pr, sell_pr = process_market(clients[exchange], exchange, pair, need_volumes[coin],
                                         conversion, config['base_currencies'])

        buys += buy_pr
        sells += sell_pr
    buys.sort(key=lambda v: v.price)
    sells.sort(key=lambda v: v.price, reverse=True)

    if not buys or not sells:
        return False

    # best_bid = buys[0].price
    # best_ask = sells[0].price
    # if coin == 'MYB':
    #     log.warning('MYB %s %s' % (best_bid, best_ask))
    # print(buys, sells)
    arb_found = False
    for bid in buys:
        for ask in sells:
            bid_price = bid.price
            ask_price = ask.price
            if bid_price * (1 + config['return']) < ask_price:
                roi = ask_price / bid_price - 1
                #print(roi, coin, buys[0], buys[1], sells[0])
                exch_fr, exch_to = bid.exchange, ask.exchange
                mkt_fr, mkt_to = bid.market, ask.market
                log.warning('coin=%s ROI=%.1f from=%s to=%s mkt_from=%s mkt_to=%s' % (coin, roi * 100,
                                                                                      exch_fr, exch_to,
                                                                                      mkt_fr, mkt_to))
                log.warning('%s %s' % (bid.deals, ask.deals))
                arb_found = True
    return arb_found

def process_market(client, exch, market, need_volume,
                   conversion, base_currencies):
    base, coin = get_base_and_coin(market, base_currencies)
    if not base:
        return [], []
    buy_prices = []
    sell_prices = []
    try:
        order_book = fetch_order_book(client.fetch_order_book, market)
    except Exception as e:
        print(e)
        print('Failed to fetch order_book %s %s' % (exch, market))
        return [], []
    bids, asks = order_book['bids'], order_book['asks']
    buy_spend, deals_buy = get_sum_on_volume(asks, need_volume, 'BUY')
    sell_spend, deals_sell = get_sum_on_volume(bids, need_volume, 'SELL')
    buy_sum_usd = buy_spend * conversion[base + '_' + 'USD']
    sell_sum_usd = sell_spend * conversion[base + '_' + 'USD']

    if buy_sum_usd:
        buy_prices.append(MarketInfo(exchange=exch, market=market,
                                     price=buy_sum_usd,
                                     volume=need_volume, deals=deals_buy))
    if sell_sum_usd:
        sell_prices.append(MarketInfo(exchange=exch, market=market,
                                      price=sell_sum_usd,
                                      volume=need_volume, deals=deals_sell))
    return buy_prices, sell_prices
