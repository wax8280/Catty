#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/3/27 23:28
import asyncio

from catty.message_queue.redis_queue import AsyncRedisPriorityQueue
from catty.scheduler.scheduler import Scheduler

loop = asyncio.get_event_loop()

scheduler_downloader_queue = AsyncRedisPriorityQueue('MySpider:SD', loop=loop)
parser_scheduler_queue = AsyncRedisPriorityQueue('MySpider:PS', loop=loop)
scheduler = Scheduler(
    parser_scheduler_queue,
    parser_scheduler_queue,
    loop=loop
)

loop.run_until_complete(scheduler_downloader_queue.conn())
loop.run_until_complete(parser_scheduler_queue.conn())
scheduler.run()
