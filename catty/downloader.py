#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/26 19:20
import asyncio
import time
import traceback
from asyncio import BaseEventLoop

import aiohttp
from aiohttp import BaseConnector

import catty.config
from catty.message_queue import AsyncRedisPriorityQueue, get_task, push_task
from catty.libs.log import Log
from catty.libs.request import Request
from catty.libs.response import Response


class DownLoader:
    def __init__(self,
                 scheduler_downloader_queue: AsyncRedisPriorityQueue,
                 downloader_parser_queue: AsyncRedisPriorityQueue,
                 loop: BaseEventLoop,
                 conn_limit: int,
                 limit_per_host: int,
                 force_close: bool):
        """
        :param scheduler_downloader_queue:      AsyncRedisPriorityQueue     The redis queue
        :param downloader_parser_queue:         AsyncRedisPriorityQueue     The redis queue
        :param loop:                            BaseEventLoop               EventLoop
        :param conn_limit:                      int                         Limit of The total number for simultaneous connections.
        :param limit_per_host                   int                         The limit for simultaneous connections to the same endpoint(host, port, is_ssl).
        """
        self.scheduler_downloader_queue = scheduler_downloader_queue
        self.downloader_parser_queue = downloader_parser_queue

        self.loop = loop
        self.conn_limit = conn_limit
        self.aio_conn = aiohttp.TCPConnector(limit=conn_limit,
                                             loop=self.loop,
                                             verify_ssl=False,
                                             limit_per_host=limit_per_host,
                                             force_close=force_close)
        self.count = 0
        self.logger = Log('Downloader')

    async def _request(self, aio_request: Request, loop: BaseEventLoop, connector: BaseConnector) -> Response:
        """The real request.It return the Response obj with status 99999 as fail"""
        t_ = time.time()
        try:
            self.logger.log_it("Downlaoding url:{} data:{}".format(aio_request.url, aio_request.data))
            async with aiohttp.request(**aio_request.dump_request(), loop=loop, connector=connector) as client:
                response = Response(
                    # TODO text accept encoding param to encode the body
                    # text= await client.text(),
                    method=client.method,
                    status=client.status,
                    cookies=client.cookies,
                    headers=client.raw_headers,
                    charset=client.charset,
                    content_type=client.content_type,
                    # history= client.history,
                    body=await client.read(),
                    use_time=time.time() - t_,
                    url=client.url,
                )
                client.close()
        except Exception as e:
            self.logger.log_it("Fail to download url:{} data:{}\nErrInfo:{}".format(aio_request.url, aio_request.data,
                                                                                    traceback.format_exc()))
            response = Response(
                status=99999,
                body=str(e),
            )

        self.count -= 1
        return response

    async def request(self, aio_request: Request, task: dict):
        """request,update the task and put it in the queue"""
        response = await self._request(aio_request, self.loop, self.aio_conn)
        if response['status'] != 99999:
            task.update({'response': response})
            await push_task(self.downloader_parser_queue, task, self.loop)
        else:
            retry = task['meta']['retry']
            retried = task.get('retried', 0)
            if retry != 0 and retried < retry:
                task.update({'retried': retried + 1})
                self.logger.log_it("Retry url:{} body:{} retried:{}".format(aio_request.url, aio_request.data, retried))
                await asyncio.sleep(task['meta']['retry_wait'], self.loop)
                await push_task(self.scheduler_downloader_queue, task, self.loop)

    async def start_crawler(self):
        """get item from queue and crawl it & push it to queue at last"""
        task = await get_task(self.scheduler_downloader_queue)
        if task is not None:
            self.count += 1
            aio_request = task['request']
            self.loop.create_task(
                self.request(
                    aio_request=aio_request,
                    task=task)
            )
            while self.count > self.conn_limit:
                await asyncio.sleep(0.5, loop=self.loop)
            self.loop.create_task(
                self.start_crawler()
            )
        else:
            await asyncio.sleep(catty.config.LOAD_QUEUE_INTERVAL, loop=self.loop)
            self.loop.create_task(
                self.start_crawler()
            )

    def run(self):
        try:
            self.loop.create_task(self.start_crawler())
            self.loop.run_forever()
        except KeyboardInterrupt:
            self.logger.log_it("Bey", level='INFO')
