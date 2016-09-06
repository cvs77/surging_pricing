#! /usr/bin/etc python
# -*- coding: utf-8 -*-
# __author__ = 'Gao Ming'
import time
from Queue import Queue
from threading import Thread

from stream_generator.utils.config import DATABASE_CONFIG
from stream_generator.utils.db_helper import StreamDataDbHelper

event_queue = Queue()
time_offset = 0
num_worker_threads = 3

db_helper = StreamDataDbHelper(DATABASE_CONFIG)


def _send_message(item):
    """
    Send the message.
    """
    print item


def worker():
    while True:
        item = event_queue.get(block=True)
        sleep_amount = float(item['ts']) - (time.time() + time_offset)
        if sleep_amount < 0:
            continue
        time.sleep(sleep_amount)
        _send_message(item)
        event_queue.task_done()

for i in range(num_worker_threads):
    t = Thread(target=worker)
    t.daemon = True
    t.start()

while True:
    pos = 0
    batch_size = 500
    if event_queue.qsize() > 100:
        time.sleep(60)
        continue
    records = db_helper.load(pos, batch_size)
    if not records:
        break
    time_offset = float(records[1]['ts']) - time.time()
    for record in records:
        event_queue.put(record)














