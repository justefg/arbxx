from tools import get_logger
from worker import process_coins
from multiprocessing import Process


log = get_logger('runner_v2.log')


def main():
    # data = Pymarketcap()._up()
    # all_markets = {coin_data['symbol']: get_markets(coin_data['symbol'], 2000)
    #                for coin_data in data}

    config = {
        'exchanges': ['bittrex', 'hitbtc', 'cryptopia', 'yobit', 'liqui',
                      'bit2c', 'huobi', 'btcchina'],
        'base_currencies': ['USD', 'USDT', 'CNY',  'BTC', 'ETH'],
        'fiat': ['USD', 'CNY'],
        'volume_threshold_usd': 200,
        'ignored': ['USDT', 'BTC', 'ETH', 'LTC', 'AEON', 'XDN', 'XMR', 'MGO', 'WAVES'],

        'return': 0.05,
        'workers_count': 1,

        'token': '402107309:AAE-V2bd9KY2kyVvbY-o6F453PnxGB5mfwY',
        'chat_id': 111827564
    }


    import json

    with open('all_markets.json', 'r') as fd:
        data = fd.read()
        all_markets = json.loads(data)
        # print(all_markets.items())
        # all_markets = json.loads(fd.read())
    # print('Calculating conversion')
    # CONVERSION = get_conversion(BASE_CURRENCIES, FIAT)

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
    workers_count = config['workers_count']
    processes = []
    all_items = list(all_markets.items())
    items_size = len(all_items) // workers_count
    iterator = 0
    print('Starting workers')
    for worker_id in range(workers_count):
        if worker_id == workers_count - 1:
            chunk = all_items[iterator:]
        else:
            chunk = all_items[iterator:iterator + items_size]
        iterator += items_size
        # process_coins(worker_id, chunk, config)
        proc = Process(target=process_coins, args=(worker_id, chunk, config))
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
