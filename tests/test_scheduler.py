#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/25 10:30

import asyncio

import asynctest

from catty.message_queue.redis_queue import AsyncRedisPriorityQueue
from catty.scheduler.scheduler import Scheduler


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
            await self.scheduler.get_task(),
            {'test1': 'testing1'}
        )
        self.assertEqual(
            await self.scheduler.get_task(),
            {'test2': 'testing2'}
        )

        self.assertEqual(
            await self.scheduler.get_task(),
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

        await self.scheduler._run_ins_func(spider_name, 'get_list', task={'next_page': 'http://www.next_page.com'})
        task = await spider_requests_q.get()
        self.assertEqual(task['request']['url'], 'http://www.next_page.com')

if __name__ == '__main__':
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
    scheduler.selector.loop = loop
    scheduler.run()

    # t1 = threading.Thread(target=scheduler.run())
    #
    # async def mock_selector():
    #     for c in range(10):
    #         await parser_scheduler_queue.put(PriorityDict({
    #             'test{}'.format(c): 'testing',
    #             'priority': c
    #         }))
