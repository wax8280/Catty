#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/26 19:20
import traceback
import aiohttp
import asyncio
from aiohttp import BaseConnector
from asyncio import Queue as AsyncQueue
from asyncio import PriorityQueue as AsyncPriorityQueue

import async_timeout
import uvloop

from catty.message_queue.redis_queue import AsyncRedisPriorityQueue
from catty.libs.request import Request


class Crawler(object):
    @staticmethod
    async def _request(aio_request: dict, loop, connector: BaseConnector):
        """The real request"""
        async with aiohttp.request(**aio_request, loop=loop, connector=connector) as client:
            # TODO try it
            response = {
                'text': await client.text(),
                'status': client.status,
                'cookies': client.cookies,
                'headers': client.headers,
                'charset': client.charset,
                'history': client.history,
                'body': await client.read(),
            }
            return response

    @staticmethod
    async def request(aio_request: dict, task, loop, connector: BaseConnector, out_q: AsyncQueue):
        """request,update the task and put it in the queue"""
        response = await Crawler._request(aio_request, loop, connector)
        priority = task['priority']
        task.update({'response': response})

        await out_q.put((
            priority, task
        ))


class DownLoader(object):
    def __init__(self,
                 scheduler_downloader_queue: AsyncRedisPriorityQueue,
                 downloader_parser_queue: AsyncRedisPriorityQueue,
                 loop=None,
                 conn_limit=1000):
        self.scheduler_downloader_queue = scheduler_downloader_queue
        self.downloader_parser_queue = downloader_parser_queue

        self.loop = loop if loop is not None else uvloop.new_event_loop()
        self.aio_conn = aiohttp.TCPConnector(limit=conn_limit)

        self.in_q = AsyncPriorityQueue()
        self.out_q = AsyncQueue()

    async def load_task_from_queue(self):
        """load task from scheduler-downloader queue"""
        try:
            return await self.downloader_parser_queue.get()
        except:
            # TODO
            # Empty or ect.
            return None

    async def push_task_to_queue(self, task):
        """push task to downloader-parser queue"""
        try:
            return await self.downloader_parser_queue.put(task)
        except:
            # TODO
            # Full or ect.
            return False

    async def start_crawler(self):
        """get item from queue and crawl it,push it to queue at last"""
        task = await self.load_task_from_queue()
        if task is not None:
            aio_request = task['request']
            self.loop.create_task(
                Crawler.request(
                    aio_request=aio_request,
                    task=task,
                    loop=self.loop,
                    connector=self.aio_conn,
                    out_q=self.out_q)
            )

        # call myself
        self.loop.create_task(
            self.start_crawler()
        )

    def loop(self):
        self.loop.run_forever()
