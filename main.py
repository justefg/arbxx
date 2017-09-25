import json

from worker import process_coins
from multiprocessing import Process


def start_workers(all_markets, config):
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


def main():
    with open('config.json', 'r') as fd:
        config = json.loads(fd.read())
    with open('all_markets.json', 'r') as fd:
        data = fd.read()
        all_markets = json.loads(data)

    start_workers(all_markets, config)


if __name__ == "__main__":
    main()
