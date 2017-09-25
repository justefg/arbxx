import logging
import time
from datetime import datetime
from tools import get_clients, get_conversion, get_need_volumes
from structs import MarketInfo, Deal, ArbOpp

from tools import get_logger, send_notifier
from tools import (get_sum_on_volume, get_base_and_coin,
                   get_usd_price, get_arb_amount)

from collections import defaultdict

log = get_logger('runner_v2.log')


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
            conversion = get_conversion(config['base'], config['fiat'])
            need_volumes = get_need_volumes(config['volume_threshold_usd'])
            clients = get_clients(config['exchanges'])

            for coin, markets in coins_markets:
                last_open_arbs = open_arbs_all_coins[coin]
                now_open_arbs = process_coin(coin, markets, clients,
                                             conversion, need_volumes, config)
                # add new arbs & update existing ones
                need_volume = need_volumes[coin]
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
                        now_price_to_buy, _ = get_sum_on_volume(order_book_from['asks'], need_volume, 'BUY')
                        now_price_to_buy *= conversion[base + '_USD']
                        base, coin = get_base_and_coin(mkt_to, config['base_currencies'])
                        now_price_to_sell, _ = get_sum_on_volume(order_book_to['bids'], need_volume, 'SELL')
                        now_price_to_sell *= conversion[base + '_USD']
                        last_to_buy = arb.price_buy
                        last_price_to_sell = arb.price_sell
                        if last_price_to_sell / now_price_to_sell - 1 > config['eps']:
                            why_closed = 'SELL'
                        else:
                            why_closed = 'BUY'

                        arb.why_closed = why_closed
                        arb.end_date = datetime.now()

                        print('Closed arb opp : coin=%s %s' % (coin, arb))
                        log.warning('Closed arb opp: coin=%s %s' % (coin, arb))
                        #log.warning('data=%s %s' % (order_book_from['asks'], order_book_to['bids']))
                        # log.warning('coin=%s volumes=%s p1=%s p2=%s p3=%s p4=%s' % (coin,
                        #                                                             need_volumes[coin],
                        #                                                             last_to_buy, last_price_to_sell,
                        #                                                             now_price_to_buy, now_price_to_sell))
                    else:
                        idx = now_open_arbs.index(arb)
                        # initial values
                        # log.warning('AHERE %s %s' % (arb.start_date, now_open_arbs[idx].end_date))
                        now_open_arbs[idx].start_date = arb.start_date
                        # log.warning('BHERE %s %s' % (now_open_arbs[idx].start_date, now_open_arbs[idx].end_date))
                        now_open_arbs[idx].price_buy = arb.price_buy
                        now_open_arbs[idx].price_sell = arb.price_sell
                open_arbs_all_coins[coin] = now_open_arbs
                for arb in now_open_arbs:
                    msg = 'Open arb opp: coin=%s %s' % (coin, arb)
                    if arb.roi > 0.08 and arb.sell_strength > 3:
                        send_notifier(config['chat_id'], config['token'], msg)
                    log.warning(msg)

    except:
        log.exception('Worker #%s fucked up' % worker_id)
        process_coins(worker_id, coins_markets, config)


def process_coin(coin, markets,
                 clients, conversion, need_volumes,
                 config):
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


    arbs = []
    need_volume = need_volumes[coin]
    for bid in buys:
        for sell in sells:
            buy_amount, sell_amount = get_arb_amount(sell.data, bid.data)
            buy_price, _ = get_sum_on_volume(sell.data, need_volume, 'BUY')
            sell_price, _ = get_sum_on_volume(bid.data, need_volume, 'SELL')

            if buy_price and buy_price * (1 + config['return']) < sell_price:
                current_date = datetime.now()
                arb_data = {
                    'e_from': sell.exchange,
                    'e_to': bid.exchange,
                    'mkt_from': sell.market,
                    'mkt_to': bid.market,
                    'start_date': current_date,
                    'end_date': current_date,
                    'price_buy': buy_price,
                    'price_sell': sell_price,
                    'buy_strength': buy_amount / need_volume,
                    'sell_strength': sell_amount / need_volume,
                }
                arb = ArbOpp(**arb_data)
                # log.warning(arb)
                # log.warning('coin=%s arb_amount=%s buy_price=%s KEKB %s' % (coin, arb_amount, buy_price, bid.data))
                # log.warning('sell_price=%s KEKS %s' % (sell_price, sell.data))
                arbs.append(arb)
                # log.warning('%s %s' % (bid.deals, ask.deals))

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
    # buy_spend, deals_buy = get_sum_on_volume(asks, need_volume, 'BUY')
    # sell_spend, deals_sell = get_sum_on_volume(bids, need_volume, 'SELL')
    # buy_sum_usd = buy_spend * conversion[base + '_' + 'USD']
    # sell_sum_usd = sell_spend * conversion[base + '_' + 'USD']
    conv = conversion[base + '_' + 'USD']
    bids = [[p * conv, v] for p, v in bids]
    asks = [[p * conv, v] for p, v in asks]

    buy_prices.append(MarketInfo(exchange=exch, market=market,
                                 volume=need_volume,
                                 data=bids))
    sell_prices.append(MarketInfo(exchange=exch, market=market,
                                  volume=need_volume,
                                  data=asks))
    return buy_prices, sell_prices
