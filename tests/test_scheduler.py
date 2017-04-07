#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/25 10:30

import asyncio
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
        cls.scheduler_downloader_queue = AsyncRedisPriorityQueue('Catty:SD', loop=cls.loop)
        cls.parser_scheduler_queue = AsyncRedisPriorityQueue('Catty:PS', loop=cls.loop)
        cls.scheduler = Scheduler(
            cls.scheduler_downloader_queue,
            cls.parser_scheduler_queue,
            loop=cls.loop
        )

    async def setUp(self):
        self.scheduler.loop = self.loop
        await self.scheduler_downloader_queue.conn()
        await self.parser_scheduler_queue.conn()

    async def tearDown(self):
        await self.scheduler_downloader_queue.clear()
        await self.parser_scheduler_queue.clear()

    async def test_load_test(self):
        await self.parser_scheduler_queue.put({'test1': 'testing1'})
        await self.parser_scheduler_queue.put({'test2': 'testing2'})
        await self.parser_scheduler_queue.put({'test3': 'testing3'})

        # ----------------test--------------------

        self.assertEqual(
            await self.scheduler.load_task(),
            {'test1': 'testing1'}
        )
        self.assertEqual(
            await self.scheduler.load_task(),
            {'test2': 'testing2'}
        )

        self.assertEqual(
            await self.scheduler.load_task(),
            {'test3': 'testing3'}
        )

    async def test_push_task(self):
        await self.scheduler.push_task(
            self.scheduler_downloader_queue,
            (1, {'test1': 'testing1'})
        )
        await self.scheduler.push_task(
            self.scheduler_downloader_queue,
            (2, {'test2': 'testing2'})
        )
        await self.scheduler.push_task(
            self.scheduler_downloader_queue,
            (3, {'test3': 'testing3'})
        )

        # ----------------test--------------------

        self.assertEqual(
            await self.scheduler.scheduler_downloader_queue.get(),
            {'test3': 'testing3'}
        )
        self.assertEqual(
            await self.scheduler.scheduler_downloader_queue.get(),
            {'test2': 'testing2'}
        )
        self.assertEqual(
            await self.scheduler.scheduler_downloader_queue.get(),
            {'test1': 'testing1'}
        )

    async def test_push_task_to_request_queue(self):
        spider_name = 'test'
        spider_requests_q = AsyncRedisPriorityQueue('{}:requests'.format(spider_name), loop=self.loop)
        await spider_requests_q.conn()
        await spider_requests_q.clear()

        await self.scheduler._push_task_to_request_queue((1, {'request1': 'test'}), spider_name)
        await self.scheduler._push_task_to_request_queue((2, {'request2': 'test'}), spider_name)

        # ----------------test--------------------

        self.assertEqual(
            await spider_requests_q.get(),
            {'request2': 'test'}
        )

        self.assertEqual(
            await spider_requests_q.get(),
            {'request1': 'test'}
        )
        await spider_requests_q.clear()

    async def test_run_ins_func(self):
        spider_name = 'spider'
        spider_requests_q = AsyncRedisPriorityQueue('{}:requests'.format(spider_name), loop=self.loop)
        await spider_requests_q.conn()
        await spider_requests_q.clear()

        await self.scheduler._run_ins_func(spider_name, 'start')
        task = await spider_requests_q.get()
        self.assertEqual(task['request']['url'], 'http://blog.vincentzhong.cn/')

        await self.scheduler._run_ins_func(spider_name, 'get_list', item={'next_page': 'http://www.next_page.com'})
        task = await spider_requests_q.get()
        self.assertEqual(task['request']['url'], 'http://www.next_page.com')


if __name__ == '__main__':
    import threading

    loop = asyncio.get_event_loop()

    scheduler_downloader_queue = AsyncRedisPriorityQueue('Catty:SD', loop=loop)
    parser_scheduler_queue = AsyncRedisPriorityQueue('Catty:PS', loop=loop)

    loop.run_until_complete(scheduler_downloader_queue.conn())
    loop.run_until_complete(parser_scheduler_queue.conn())

    scheduler = Scheduler(
        scheduler_downloader_queue,
        parser_scheduler_queue,
        loop
    )
    scheduler.instantiate_spider()

    loop.create_task(scheduler.run())
    loop.run_forever()

    # t1 = threading.Thread(target=scheduler.run())
    #
    # async def mock_selector():
    #     for c in range(10):
    #         await parser_scheduler_queue.put(PriorityDict({
    #             'test{}'.format(c): 'testing',
    #             'priority': c
    #         }))
