#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/3/9 17:06

import asynctest
from catty.message_queue.redis_queue import AsyncRedisPriorityQueue
import uvloop
import asyncio
from catty.downloader.downloader import DownLoader
from catty.libs.request import Request


class Test(asynctest.TestCase):
    use_default_loop = True

    async def setUp(self):
        pass

    async def tearDown(self):
        pass


def test_downloader():
    loop = uvloop.new_event_loop()
    scheduler_downloader_queue = AsyncRedisPriorityQueue('test:sd', loop, queue_maxsize=10000)
    downloader_parser_queue = AsyncRedisPriorityQueue('test:dp', loop, queue_maxsize=10000)

    loop.run_until_complete(scheduler_downloader_queue.conn())
    loop.run_until_complete(downloader_parser_queue.conn())

    downloader = DownLoader(scheduler_downloader_queue, downloader_parser_queue, loop)

    async def put_test_data():
        for i in range(10):
            task = {
                'request': Request(url='http://blog.vincentzhong.cn/a={}'.format(i)).dump_request(),
                'priority': 1
            }
            await scheduler_downloader_queue.put(task)

    async def print_result(q):
        last = 0
        while True:
            print("{} item/s".format(await q.qsize() - last))
            last = await q.qsize()
            await asyncio.sleep(1, loop=loop)

    loop.create_task(put_test_data())
    loop.create_task(downloader.start_crawler())
    loop.create_task(print_result(downloader_parser_queue))
    loop.run_forever()


if __name__ == '__main__':
    test_downloader()
