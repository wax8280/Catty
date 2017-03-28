#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/3/27 23:29
import asyncio

from catty.message_queue.redis_queue import AsyncRedisPriorityQueue
from catty.parser.parser import Parser

loop = asyncio.get_event_loop()
downloader_parser_queue = AsyncRedisPriorityQueue('MySpider:DP', loop=loop)
parser_scheduler_queue = AsyncRedisPriorityQueue('MySpider:PS', loop=loop)

loop.run_until_complete(parser_scheduler_queue.conn())
loop.run_until_complete(downloader_parser_queue.conn())

parser = Parser(
    downloader_parser_queue,
    parser_scheduler_queue,
    loop
)

parser.run()
