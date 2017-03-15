#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/26 19:20
import traceback
import aiohttp
import asyncio
import async_timeout

from catty.message_queue.redis_queue import RedisPriorityQueue
from catty.libs.request import Request


class Crawler(object):
    @staticmethod
    def make_aio_request(request: Request):
        return aiohttp.request(
            method=request.method,
            url=request.url,
            data=request.data,
            headers=request.headers,
            cookies=request.cookies,
            allow_redirects=request.allow_redirects,
        )

class DownLoader(object):
    def __init__(self,
                 scheduler_downloader_queue: RedisPriorityQueue,
                 downloader_parser_queue: RedisPriorityQueue):
        self.scheduler_downloader_queue = scheduler_downloader_queue
        self.downloader_parser_queue = downloader_parser_queue

        self._downloaded_task = []
        self._task_from_scheduler = []
        self.loop = asyncio.get_event_loop()

    def _get_task_from_scheduler_downloader_queue(self):
        return self.scheduler_downloader_queue.get_nowait()

    def _push_task_to_downloader_parser_queue(self):
        try:
            t = self._downloaded_task.pop()
        except IndexError:
            return

        try:
            self.downloader_parser_queue.put_nowait(t)
        except:
            traceback.print_exc()
            self._downloaded_task.append(t)
            # TODO log it

    def load_task_from_queue(self):
        """load task from scheduler-downloader queue in a loop,append it to self._task_from_scheduler"""
        try:
            self._task_from_scheduler.append(
                self._get_task_from_scheduler_downloader_queue()
            )
        except:
            traceback.print_exc()

    def crawler(self):
        """start fetch"""
        pass

    def loop(self):
        self.loop.run_until_complete()
