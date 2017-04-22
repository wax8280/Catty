#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/3/27 23:29

from catty.message_queue import AsyncRedisPriorityQueue
from catty.config import QUEUE, DOWNLOADER
from catty.downloader import DownLoader
from catty.libs.utils import get_eventloop

if __name__ == '__main__':
    loop = get_eventloop()

    scheduler_downloader_queue = AsyncRedisPriorityQueue(
        'Catty:Scheduler-Downloader', loop, queue_maxsize=QUEUE['MAX_SIZE'])
    downloader_parser_queue = AsyncRedisPriorityQueue(
        'Catty:Downloader-Parser', loop, queue_maxsize=QUEUE['MAX_SIZE'])

    loop.run_until_complete(scheduler_downloader_queue.conn())
    loop.run_until_complete(downloader_parser_queue.conn())

    downloader = DownLoader(
        scheduler_downloader_queue,
        downloader_parser_queue,
        loop,
        conn_limit=DOWNLOADER['CONN_LIMIT'],
        limit_per_host=DOWNLOADER['LIMIT_PER_HOST'],
        force_close=DOWNLOADER['FORCE_CLOSE'])

    downloader.run()
