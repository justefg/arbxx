import os
from datetime import datetime
import telegram

CHAT_ID = '111827564'
TOKEN = '402107309:AAE-V2bd9KY2kyVvbY-o6F453PnxGB5mfwY'


def prepare_result():
    today = datetime.now()
    y, m, d, h = today.year, today.month, today.day, today.hour
    cmd = "grep '%02d-%02d-%02d %02d:.*Closed' runner_v2.log | grep -v USD > result" % (y, m, d, h)
    os.system(cmd)


def get_key(line):
    data = line.split(' ')
    # mkt_from = data[10].split('=')[1]
    # mkt_to = data[11].split('=')[1]
    # e_from = data[12].split('=')[1]
    # e_to = data[13].split('=')[1]
    key = ' '.join([data[10], data[11], data[12], data[13]])
    return key


from collections import Counter


def process_result():
    counter = Counter()
    with open('result', 'r') as fd:
        lines = fd.readlines()
        for line in lines:
            key = get_key(line)
            counter[key] += 1

    bot = telegram.Bot(TOKEN)
    for k, v in counter.most_common():
        msg = k + ': ' + str(v)
        bot.send_message(chat_id=CHAT_ID, text=msg)

prepare_result()
process_result()
