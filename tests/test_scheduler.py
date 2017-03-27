#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/25 10:30

import asyncio
import threading
import time
import unittest

import asynctest

from catty.libs.utils import PriorityDict
from catty.message_queue.redis_queue import AsyncRedisPriorityQueue
from catty.scheduler.scheduler import Scheduler


class Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.loop = asyncio.get_event_loop()
        cls.scheduler_downloader_queue = AsyncRedisPriorityQueue('MySpider:SD', loop=cls.loop)
        cls.parser_scheduler_queue = AsyncRedisPriorityQueue('MySpider:PS', loop=cls.loop)
        cls.scheduler = Scheduler(
            cls.parser_scheduler_queue,
            cls.parser_scheduler_queue,
            loop=cls.loop
        )

    def test_spider_start_method(self):
        self.scheduler.instantiate_spider()
        self.scheduler.make_task_from_start()

        # print(self.scheduler.task_to_downloader.get())
        self.assertIn(
            "'spider_name': 'MySpider'",
            self.scheduler.task_to_downloader.get()
        )

    #
    def test_make_tasks(self):
        self.scheduler.instantiate_spider()
        self.scheduler.make_task_from_start()

        # assume the task downloaded
        task = self.scheduler.task_to_downloader.get()
        # parser中，有多少个callback line就复制多少个 task
        # 因为不同的parser生成不同的item，不同的item生成不同的task
        # must return a dict
        task.update({'parser': {'item':
                                    {'content_url': ['url1', 'url2'],
                                     'next_page': 'next_page_url'}
                                }})

        self.scheduler.task_from_parser.put(task)

        # start test
        self.scheduler.make_tasks()

        self.assertIn(
            "'tid': 'ecd129a62d6c6a47dc1fef5118d8cdc7'",
            str(self.scheduler.task_to_downloader.get()),
        )
        self.assertIn(
            "'tid': 'df917c4d6b04e758b7051ce14f0e6db2'",
            str(self.scheduler.task_to_downloader.get()),
        )
        self.assertIn(
            "'tid': '8eef71b0c86ce59ea7804e1126aa55a5'",
            str(self.scheduler.task_to_downloader.get())
        )

    def test_select_task(self):
        self.scheduler.instantiate_spider()
        self.scheduler.make_task_from_start()
        task = self.scheduler.task_to_downloader.get()
        task.update({'parser': {'item':
                                    {'content_url': ['url1', 'url2'],
                                     'next_page': 'next_page_url'}
                                },
                     # 'scheduler': {'exetime': time.time() + 2}
                     })
        self.scheduler.task_from_parser.put(task)
        self.scheduler.make_tasks()
        self.scheduler.select_task()
        print(self.scheduler.selected_task.qsize())
        # time.sleep(2)
        # self.scheduler.select_task()
        # print(self.scheduler.selected_task.get())


class TestAsyncScheduler(asynctest.TestCase):
    use_default_loop = True

    @classmethod
    def setUpClass(cls):
        cls.scheduler_downloader_queue = AsyncRedisPriorityQueue('MySpider:SD', loop=cls.loop)
        cls.parser_scheduler_queue = AsyncRedisPriorityQueue('MySpider:PS', loop=cls.loop)
        cls.scheduler = Scheduler(
            cls.parser_scheduler_queue,
            cls.parser_scheduler_queue,
            loop=cls.loop
        )

    async def setUp(self):
        self.scheduler.loop = self.loop
        await self.scheduler_downloader_queue.conn()
        await self.parser_scheduler_queue.conn()

    async def tearDown(self):
        pass

    async def test_load_test(self):
        await self.parser_scheduler_queue.put({'test1': 'testing1'})
        await self.parser_scheduler_queue.put({'test2': 'testing2'})
        await self.parser_scheduler_queue.put({'test3': 'testing3'})

        # ----------------test--------------------
        await self.scheduler.load_task()
        self.assertEqual(
            self.scheduler.task_from_parser.get(),
            {'test1': 'testing1'}
        )
        await self.scheduler.load_task()
        self.assertEqual(
            self.scheduler.task_from_parser.get(),
            {'test2': 'testing2'}
        )
        await self.scheduler.load_task()
        self.assertEqual(
            self.scheduler.task_from_parser.get(),
            {'test3': 'testing3'}
        )

    async def test_push_task(self):
        self.scheduler.selected_task.put((1, {'test1': 'testing1'}))
        self.scheduler.selected_task.put((2, {'test2': 'testing2'}))
        self.scheduler.selected_task.put((3, {'test3': 'testing3'}))

        await self.scheduler.push_task()
        await self.scheduler.push_task()
        await self.scheduler.push_task()

        self.assertEqual(
            await self.scheduler.scheduler_downloader_queue.get(),
            (1, {'test1': 'testing1'})
        )
        self.assertEqual(
            await self.scheduler.scheduler_downloader_queue.get(),
            (2, {'test2': 'testing2'})
        )
        self.assertEqual(
            await self.scheduler.scheduler_downloader_queue.get(),
            (3, {'test3': 'testing3'})
        )


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    scheduler_downloader_queue = AsyncRedisPriorityQueue('MySpider:SD', loop=loop)
    parser_scheduler_queue = AsyncRedisPriorityQueue('MySpider:PS', loop=loop)

    loop.run_until_complete(scheduler_downloader_queue.conn())
    loop.run_until_complete(parser_scheduler_queue.conn())

    scheduler = Scheduler(
        scheduler_downloader_queue,
        parser_scheduler_queue,
        loop
    )

    scheduler.instantiate_spider()

    async def put_task():
        for c in range(10):
            await parser_scheduler_queue.put(PriorityDict({
                'test{}'.format(c): 'testing',
                'priority': c
            }))


    def test_run_async_queue():
        # 该测试看起来并没有真正实现异步，其实是因为
        def mock_select():
            while True:
                scheduler.selected_task.put(
                    # 略过old task - new task 的过程
                    scheduler.task_from_parser.get()
                )

        t1 = threading.Thread(target=scheduler.run_async_queue())
        t2 = threading.Thread(target=mock_select)
        t1.start()
        t2.start()

        loop.create_task(put_task())
        loop.run_forever()

    test_run_async_queue()

    def test_run():
        t1 = threading.Thread(target=scheduler.run_async_queue)
        t2 = threading.Thread(target=scheduler.run_task_maker)
        t3 = threading.Thread(target=scheduler.run_selector)

        t1.start()
        t2.start()
        t3.start()

        loop.run_forever()

    # test_run()