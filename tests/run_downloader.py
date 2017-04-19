#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/3/27 23:29
import asyncio

from catty.downloader.downloader import DownLoader
from catty.message_queue.redis_queue import AsyncRedisPriorityQueue

loop = asyncio.get_event_loop()

scheduler_downloader_queue = AsyncRedisPriorityQueue('MySpider:SD', loop, queue_maxsize=10000)
downloader_parser_queue = AsyncRedisPriorityQueue('MySpider:DP', loop, queue_maxsize=10000)

loop.run_until_complete(scheduler_downloader_queue.conn())
loop.run_until_complete(downloader_parser_queue.conn())

downloader = DownLoader(
    scheduler_downloader_queue,
    downloader_parser_queue,
    loop,
    1000,
    100,
    True)

downloader.run()
