#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/26 19:57
# !/usr/bin/env python
import asynctest

from catty.message_queue import AsyncRedisPriorityQueue
from catty.parser import Parser
from catty import DOWNLOADER_PARSER, PARSER_SCHEDULER
import catty.config
import asyncio


class TestParser(asynctest.TestCase):
    use_default_loop = True

    @classmethod
    def setUpClass(cls):
        from catty.libs.tasker import Tasker
        from tests.spider.mock_spider import Spider

        cls.downloader_parser_queue = AsyncRedisPriorityQueue('MySpider:DP', loop=cls.loop)
        cls.parser_scheduler_queue = AsyncRedisPriorityQueue('MySpider:PS', loop=cls.loop)

        cls.parser = Parser(
            cls.downloader_parser_queue,
            cls.parser_scheduler_queue,
            cls.loop
        )
        myspider = Spider()
        tasker = Tasker()
        cls.parser.spider_started.add(myspider.name)

        cls.task = tasker.make_task(myspider.start())

        cls.mock_response_task = tasker.make_task(myspider.start())
        cls.mock_response_task['response'] = {'body': 'this is a mock response', 'status': 200}

    async def setUp(self):
        self.parser.loop = self.loop
        await self.downloader_parser_queue.conn()
        await self.parser_scheduler_queue.conn()

    async def tearDown(self):
        pass

    async def test_run_ins_func(self):
        await self.parser._run_ins_func('mock_spider', 'mock_parser_with_response', self.mock_response_task)
        t1 = await self.parser.parser_scheduler_queue.get()
        self.assertAlmostEqual(self.mock_response_task, t1)

    async def test_make_tasks_return_dict(self):
        await self.downloader_parser_queue.put(self.mock_response_task)

        # -----------------test-------------------
        await self.parser.make_tasks()
        await asyncio.sleep(0.5, self.loop)
        t1 = await self.parser.parser_scheduler_queue.get()
        self.assertAlmostEqual(self.mock_response_task, t1)

    async def test_dump_load_tasks(self):
        import os
        await self.downloader_parser_queue.put(self.mock_response_task)
        await self.parser_scheduler_queue.put(self.mock_response_task)

        await self.parser.dump_tasks(DOWNLOADER_PARSER)
        await self.parser.dump_tasks(PARSER_SCHEDULER)

        await asyncio.sleep(0.5, loop=self.loop)

        self.assertTrue(os.path.exists(os.path.join(
            os.path.join(catty.config.PERSISTENCE['DUMP_PATH'], '{}_{}'.format(self.parser.name, DOWNLOADER_PARSER)),
            self.mock_response_task['spider_name'])))
        self.assertTrue(os.path.exists(os.path.join(
            os.path.join(catty.config.PERSISTENCE['DUMP_PATH'], '{}_{}'.format(self.parser.name, PARSER_SCHEDULER)),
            self.mock_response_task['spider_name'])))

        await self.parser.load_tasks(DOWNLOADER_PARSER, self.mock_response_task['spider_name'])
        await self.parser.load_tasks(PARSER_SCHEDULER, self.mock_response_task['spider_name'])

        downloader_parser_task = await self.downloader_parser_queue.get()
        parser_scheduler_task = await self.parser_scheduler_queue.get()

        self.assertAlmostEqual(self.mock_response_task, downloader_parser_task)
        self.assertAlmostEqual(self.mock_response_task, parser_scheduler_task)

        if __name__ == '__main__':
            pass
