#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/26 10:05
import asyncio
import inspect
import time
import traceback
from copy import deepcopy
from queue import PriorityQueue

from catty.exception import *
from catty.message_queue.redis_queue import AsyncRedisPriorityQueue

try:
    import uvloop

    LOOP = uvloop.uvloop.new_event_loop()
except ImportError:
    LOOP = asyncio.get_event_loop()


class Parser(object):
    LOAD_SPIDER_INTERVAL = 0.3

    def __init__(self,
                 downloader_parser_queue: AsyncRedisPriorityQueue,
                 parser_scheduler_queue: AsyncRedisPriorityQueue,
                 loop=LOOP,
                 ):

        self.downloader_parser_queue = downloader_parser_queue
        self.parser_scheduler_queue = parser_scheduler_queue

        self._count_pre_loop = 30

        self.inner_in_q = PriorityQueue()
        self.inner_ou_q = PriorityQueue()

        self.loop = loop

    async def conn_redis(self):
        """conn the redis"""
        await self.downloader_parser_queue.conn()
        await self.parser_scheduler_queue.conn()

    async def _get_task(self):
        """load TASK from downloader_parser queue"""
        return await self.downloader_parser_queue.get()

    async def _put_task(self, item):
        """put TASK to parser-scheduler queue"""
        return await self.parser_scheduler_queue.put(item)

    async def load_task(self):
        """load TASK from downloader-parser queue,put it in Parser own queue"""
        self.inner_in_q.put(
            await self._get_task()
        )

    async def put_task(self):
        """put TASK from Parser own queue to parser-scheduler queue"""
        task = None
        while not task:
            try:
                task = self.inner_ou_q.get_nowait()
            except:
                asyncio.sleep(0.5, loop=self.loop)
        await self._put_task(
            task
        )

    def _run_ins_func(self, func, task):
        """run the spider_ins boned method to parser it and return a item,and append it to self._task"""
        _response = task['response']
        _signature = inspect.signature(func).parameters
        assert 'response' in _signature
        if 'task' in _signature:
            parser_return = func(_response, task=task)
        else:
            parser_return = func(_response)

        # if return a dict(normal)
        if isinstance(parser_return, dict):
            task['parser'].update({'item': parser_return})
            self.inner_ou_q.put(task)

    def make_tasks(self):
        """get old task from self._task_from_downloader and make new task to append it to self._task_to_scheduler"""
        try:
            task = self.inner_in_q.get()
        except IndexError:
            return

        callback = task['callback']
        for each_callback in callback:
            each_task = deepcopy(task)
            parser_func = each_callback.get('parser', None)
            # TODO 判断result_pipiline_func是否是可迭代的（可以是list）
            if parser_func:
                try:
                    self._run_ins_func(parser_func, each_task)
                except Retry_current_task:
                    # handle it like a new task
                    self.inner_ou_q.put(each_task)
                except:
                    traceback.print_exc()
                    pass

    async def _loop(self):
        __c__ = 0
        while not self.downloader_parser_queue and __c__ < self._count_pre_loop:
            __c__ += 1
            await self.load_task()
            self.make_tasks()

    def run(self):
        while True:
            self._loop()
            time.sleep(self.LOAD_SPIDER_INTERVAL)
