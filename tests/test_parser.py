#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/26 19:57
# !/usr/bin/env python

from copy import deepcopy

import asynctest

from catty.message_queue import AsyncRedisPriorityQueue
from catty.parser import Parser
from catty.libs.response import Response


class TestParser(asynctest.TestCase):
    use_default_loop = True

    @classmethod
    def setUpClass(cls):
        from catty.demo.spider import Spider
        from catty.scheduler import Tasker

        cls.downloader_parser_queue = AsyncRedisPriorityQueue('MySpider:DP', loop=cls.loop)
        cls.parser_scheduler_queue = AsyncRedisPriorityQueue('MySpider:PS', loop=cls.loop)

        cls.parser = Parser(
            cls.downloader_parser_queue,
            cls.parser_scheduler_queue,
            cls.loop
        )
        myspider = Spider()
        tasker = Tasker()

        cls.task = tasker._make_task(myspider.start())

    async def setUp(self):
        self.parser.loop = self.loop
        await self.downloader_parser_queue.conn()
        await self.parser_scheduler_queue.conn()

    async def tearDown(self):
        pass

    async def test_make_tasks(self):
        task = deepcopy(self.task)

        # mock downloader
        task.update({
            'tid': 2,
            'response': Response(
                status=200,
                method='GET',
                headers={'header': 'header'},
                use_time=0.5,
                cookies={'cookies': 'cookies'},
                content_type='ct',
                charset='c',
                body='hello world'
            )
        })

        await self.downloader_parser_queue.put(task)

        # -----------------test-------------------
        await self.parser.make_tasks()
        task = await self.parser.parser_scheduler_queue.get()
        task_2 = await self.parser.parser_scheduler_queue.get()
        self.assertEqual(
            task['parser']['item']['content_url'],
            ['url1', 'url2']
        )
        self.assertEqual(
            task_2['parser']['item']['content'],
            'content in parser_content_page'
        )


if __name__ == '__main__':
    pass
