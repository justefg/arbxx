import logging
import time
from datetime import datetime
from tools import get_clients, get_conversion, get_need_volumes
from structs import MarketInfo, Deal, ArbOpp

from tools import get_logger
from collections import defaultdict

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
    open_arbs_all_coins = defaultdict(lambda: [])
    try:
        while True:
            log.warning('Worker #%s processing coins' % worker_id)
            conversion = get_conversion(config['base_currencies'], config['fiat'])
            need_volumes = get_need_volumes(config['volume_threshold_usd'])
            clients = get_clients(config['exchanges'])
            for coin, markets in coins_markets:
                last_open_arbs = open_arbs_all_coins[coin]
                now_open_arbs = process_coin(coin, markets, clients,
                                             conversion, need_volumes, config)
                # add new arbs & update existing ones
                for arb in last_open_arbs:
                    if arb not in now_open_arbs:
                        # arb opportunity has closed
                        # whY ?
                        client_from = clients[arb.e_from]
                        client_to = clients[arb.e_to]
                        mkt_from = arb.mkt_from
                        mkt_to = arb.mkt_to

                        try:
                            order_book_from = fetch_order_book(client_from.fetch_order_book, mkt_from)
                        except Exception as e:
                            print('Failed to fetch order_book %s %s' % (arb.e_from, mkt_from))
                            continue

                        try:
                            order_book_to = fetch_order_book(client_to.fetch_order_book, mkt_to)
                        except Exception as e:
                            print('Failed to fetch order_book %s %s' % (arb.e_to, mkt_to))
                            continue
                        base, coin = get_base_and_coin(mkt_from, config['base_currencies'])
                        now_price_to_buy, _ = get_sum_on_volume(order_book_from['asks'], need_volumes[coin], 'BUY')
                        now_price_to_buy *= conversion[base + '_USD']
                        base, coin = get_base_and_coin(mkt_to, config['base_currencies'])
                        now_price_to_sell, _ = get_sum_on_volume(order_book_to['bids'], need_volumes[coin], 'SELL')
                        now_price_to_sell *= conversion[base + '_USD']
                        last_to_buy = arb.price_buy
                        last_price_to_sell = arb.price_sell
                        if last_price_to_sell / now_price_to_sell - 1 > config['eps']:
                            why_closed = 'SELL'
                        else:
                            why_closed = 'BUY'

                        arb.why_closed = why_closed
                        arb.end_date = datetime.now()

                        print('Arb opp closed: coin=%s %s' % (coin, arb))
                        log.warning('Arb opp closed: coin=%s %s' % (coin, arb))
                        #log.warning('data=%s %s' % (order_book_from['asks'], order_book_to['bids']))
                        log.warning('coin=%s volumes=%s p1=%s p2=%s p3=%s p4=%s' % (coin,
                                                                                    need_volumes[coin],
                                                                                    last_to_buy, last_price_to_sell,
                                                                                    now_price_to_buy, now_price_to_sell))
                    else:
                        idx = now_open_arbs.index(arb)
                        # initial values
                        now_open_arbs[idx].start_date = arb.start_date
                        now_open_arbs[idx].price_buy = arb.price_buy
                        now_open_arbs[idx].price_sell = arb.price_sell
                open_arbs_all_coins[coin] = now_open_arbs
    except:
        log.exception('Worker #%s fucked up' % worker_id)


def process_coin(coin, markets,
                 clients, conversion, need_volumes,
                 config):
    # coin, markets = coin_markets
    if coin in config['ignored']:
        return []
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
        buy_pr, sell_pr = process_market(clients[exchange], exchange, pair,
                                         need_volumes[coin],
                                         conversion, config['base_currencies'])

        buys += buy_pr
        sells += sell_pr
    buys.sort(key=lambda v: v.price)
    sells.sort(key=lambda v: v.price, reverse=True)
    if not buys or not sells:
        return []

    # best_bid = buys[0].price
    # best_ask = sells[0].price
    # if coin == 'MYB':
    #     log.warning('MYB %s %s' % (best_bid, best_ask))
    # print(buys, sells)
    arbs = []
    for bid in buys:
        for ask in sells:
            bid_price = bid.price
            ask_price = ask.price
            if bid_price * (1 + config['return']) < ask_price:
                current_date = datetime.now()
                arb_data = {
                    'e_from': bid.exchange,
                    'e_to': ask.exchange,
                    'mkt_from': bid.market,
                    'mkt_to': ask.market,
                    'start_date': current_date,
                    'end_date': current_date,
                    'price_buy': bid_price,
                    'price_sell': ask_price
                }
                arb = ArbOpp(**arb_data)
                arbs.append(arb)
                log.warning('coin=%s %s' % (coin, arb))
                log.warning('%s %s' % (bid.deals, ask.deals))

    return arbs


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
