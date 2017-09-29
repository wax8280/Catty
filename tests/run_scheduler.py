#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/3/27 23:28
from catty.message_queue import AsyncRedisPriorityQueue
from catty.scheduler import Scheduler
from catty.config import QUEUE
from catty.libs.utils import get_eventloop

loop = get_eventloop()
if __name__ == '__main__':
    scheduler_downloader_queue = AsyncRedisPriorityQueue(
        'Catty:Scheduler-Downloader', loop=loop, queue_maxsize=QUEUE['MAX_SIZE'])
    parser_scheduler_queue = AsyncRedisPriorityQueue(
        'Catty:Parser-Scheduler', loop=loop, queue_maxsize=QUEUE['MAX_SIZE'])
    loop.run_until_complete(scheduler_downloader_queue.conn())
    loop.run_until_complete(parser_scheduler_queue.conn())

    scheduler = Scheduler(
        scheduler_downloader_queue,
        parser_scheduler_queue,
        loop,
        'master_scheduler'
    )

    scheduler.run()
