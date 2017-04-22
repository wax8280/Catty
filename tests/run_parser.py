#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/3/27 23:29
import catty.config
from catty.message_queue import AsyncRedisPriorityQueue
from catty.parser import Parser
from catty.libs.utils import get_eventloop

if __name__ == '__main__':
    loop = get_eventloop()
    downloader_parser_queue = AsyncRedisPriorityQueue(
        'Catty:Downloader-Parser', loop=loop, queue_maxsize=catty.config.QUEUE['MAX_SIZE'])
    parser_scheduler_queue = AsyncRedisPriorityQueue(
        'Catty:Parser-Scheduler', loop=loop, queue_maxsize=catty.config.QUEUE['MAX_SIZE'])

    loop.run_until_complete(parser_scheduler_queue.conn())
    loop.run_until_complete(downloader_parser_queue.conn())

    parser = Parser(
        downloader_parser_queue,
        parser_scheduler_queue,
        loop
    )

    parser.run()
