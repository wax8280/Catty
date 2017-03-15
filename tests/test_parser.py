#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/26 19:57
# !/usr/bin/env python

from copy import deepcopy

import asynctest

from catty.libs.response import Response
from catty.message_queue.redis_queue import AsyncRedisPriorityQueue
from catty.parser.parser import Parser


class TestParser(asynctest.TestCase):
    use_default_loop = True

    @classmethod
    def setUpClass(cls):
        from catty.demo.spider import MySpider
        from catty.scheduler.tasker import Tasker

        cls.downloader_parser_queue = AsyncRedisPriorityQueue('MySpider:DP', loop=cls.loop)
        cls.parser_scheduler_queue = AsyncRedisPriorityQueue('MySpider:PS', loop=cls.loop)

        cls.parser = Parser(
            cls.downloader_parser_queue,
            cls.parser_scheduler_queue,
            cls.loop
        )
        myspider = MySpider()
        tasker = Tasker()

        cls.task = tasker._make_task(myspider.start())

    async def setUp(self):
        await self.downloader_parser_queue.conn()
        await self.parser_scheduler_queue.conn()

    async def tearDown(self):
        pass

    async def test_load_task(self):
        task = deepcopy(self.task)
        # downloader
        task.update({
            'tid': 1,
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
        await self.parser.load_task()
        self.assertEqual(
            self.parser.inner_in_q.get()['tid'],
            1
        )

    async def test_put_task(self):
        task = deepcopy(self.task)
        # downloader
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
        await self.parser.load_task()

        self.parser.inner_ou_q.put(
            self.parser.inner_in_q.get()
        )

        await self.parser.put_task()

        result = await self.parser_scheduler_queue.get()
        self.assertEqual(
            result['tid'],
            2
        )

    async def test_make_tasks(self):
        task = deepcopy(self.task)
        # downloader
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
        await self.parser.load_task()
        self.parser.make_tasks()
        self.assertEqual(
            self.parser.inner_ou_q.get()['parser']['item']['content_url'],
            'content_urls in parser_content_page'
        )
        self.assertEqual(
            self.parser.inner_ou_q.get()['parser']['item']['content'],
            'content in parser_content_page'
        )


if __name__ == '__main__':
    pass
