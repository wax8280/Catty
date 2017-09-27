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
                 conn_limit: int):
        """
        :param scheduler_downloader_queue:The redis queue
        :param downloader_parser_queue:The redis queue
        :param loop:EventLoop
        :param conn_limit:Limit of The total number for simultaneous connections.
        # :param limit_per_host:The limit for simultaneous connections to the same endpoint(host, port, is_ssl).
        """
        self.scheduler_downloader_queue = scheduler_downloader_queue
        self.downloader_parser_queue = downloader_parser_queue

        self.loop = loop
        self.conn_limit = conn_limit

        # using in conn_limit
        self.count = 0
        self.logger = Log('Downloader')

    async def _request(self, aio_request: Request, loop: BaseEventLoop) -> Response:
        """The real request.It return the Response obj with status 99999 as fail"""
        t_ = time.time()
        self.logger.log_it("Downloading url:{} data:{}".format(aio_request.url, aio_request.data))
        try:
            async with aiohttp.ClientSession(loop=loop) as session:
                if aio_request.method == 'GET':
                    async with session.get(**aio_request.dump_request()) as client:
                        body = await client.read()
                elif aio_request.method == 'POST':
                    async with session.post(**aio_request.dump_request()) as client:
                        body = await client.read()
                elif aio_request.method == 'PUT':
                    async with session.put(**aio_request.dump_request()) as client:
                        body = await client.read()
                elif aio_request.method == 'DELETE':
                    async with session.delete(**aio_request.dump_request()) as client:
                        body = await client.read()
                elif aio_request.method == 'HEAD':
                    async with session.head(**aio_request.dump_request()) as client:
                        body = await client.read()
                elif aio_request.method == 'OPTIONS':
                    async with session.options(**aio_request.dump_request()) as client:
                        body = await client.read()
                elif aio_request.method == 'PATCH':
                    async with session.path(**aio_request.dump_request()) as client:
                        body = await client.read()
                else:
                    self.logger.log_it("Not a vaild method.Request:{}".format(aio_request), level='INFO')
                    return Response(status=-1, body=str("Not a vaild method.Request:{}".format(aio_request)), )

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
                    body=body,
                    use_time=time.time() - t_,
                    url=client.url,
                )

        except Exception as e:
            self.logger.log_it("Fail to download url:{} data:{} ErrInfo:{}".format(aio_request.url, aio_request.data,
                                                                                    traceback.format_exc()))
            response = Response(status=99999, body=str(e), )

        self.count -= 1
        return response

    async def fail_callback(self, task: dict, aio_request: Request):
        retry = task['meta']['retry']
        retried = task.get('retried', 0)
        if retry != 0 and retried < retry:
            task.update({'retried': retried + 1})
            self.logger.log_it("Retry url:{} body:{} retried:{}".format(aio_request.url, aio_request.data, retried))
            # retry wait
            await asyncio.sleep(task['meta']['retry_wait'], self.loop)
            await push_task(self.scheduler_downloader_queue, task, self.loop)

    async def success_callback(self, task: dict, response: Response):
        task.update({'response': response})
        await push_task(self.downloader_parser_queue, task, self.loop)

    async def request(self, aio_request: Request, task: dict):
        """request,update the task and put it in the queue"""
        response = await self._request(aio_request, self.loop)
        # TODO:99999 means catch exception during request(or we should uniform the status code and write a doc)
        if response['status'] != 99999:
            # success
            await self.success_callback(task, response)
        elif response['status'] == -1:
            # -1 means ignore this status
            pass
        else:
            # fail
            await self.fail_callback(task, aio_request)

    async def start_crawler(self):
        """get item from queue and crawl it & push it to queue at last"""
        task = await get_task(self.scheduler_downloader_queue)
        if task is not None:
            self.count += 1
            aio_request = task['request']
            self.loop.create_task(self.request(aio_request=aio_request, task=task))

            # The limit of concurrent request
            while self.count > self.conn_limit:
                await asyncio.sleep(0.5, loop=self.loop)

            self.loop.create_task(self.start_crawler())
        else:
            # If the queue is empty,wait and try again.
            await asyncio.sleep(catty.config.LOAD_QUEUE_INTERVAL, loop=self.loop)
            self.loop.create_task(self.start_crawler())

    def run(self):
        try:
            self.loop.create_task(self.start_crawler())
            self.loop.run_forever()
        except KeyboardInterrupt:
            self.logger.log_it("Bye!", level='INFO')
