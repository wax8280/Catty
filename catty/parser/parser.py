#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/26 10:05
import asyncio
import inspect
import traceback
from copy import deepcopy
from queue import PriorityQueue

from catty.config import CONFIG
from catty.exception import *
from catty.libs.handle_module import SpiderModuleHandle
from catty.message_queue.redis_queue import AsyncRedisPriorityQueue


class Parser(object):
    LOAD_SPIDER_INTERVAL = 0.3

    def __init__(self,
                 downloader_parser_queue: AsyncRedisPriorityQueue,
                 parser_scheduler_queue: AsyncRedisPriorityQueue,
                 loop,
                 ):

        self.downloader_parser_queue = downloader_parser_queue
        self.parser_scheduler_queue = parser_scheduler_queue

        self._count_pre_loop = 30

        self.inner_in_q = PriorityQueue()
        self.inner_ou_q = PriorityQueue()

        self.loop = loop

        self.spider_module_handle = SpiderModuleHandle(CONFIG['SPIDER_PATH'])
        self.spider_module_handle.to_instance_spider()

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

    def _run_ins_func(self, spider_name, method_name, task):
        """run the spider_ins boned method to parser it and return a item,and append it to self._task"""
        _response = task['response']
        try:
            spider_ins = self.spider_module_handle.spider_instantiation[spider_name]
        except IndexError:
            print('No this Spider or had not instance')

        method = spider_ins.__getattribute__(method_name)

        _signature = inspect.signature(method).parameters
        assert 'response' in _signature
        if 'task' in _signature:
            parser_return = method(_response, task=task)
        else:
            parser_return = method(_response)

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
        spider_name = task['spider_name']
        for callback_method_name in callback:
            each_task = deepcopy(task)
            parser_method_name = callback_method_name.get('parser', None)
            # TODO 判断result_pipiline_func是否是可迭代的（可以是list）
            if parser_method_name:
                try:
                    self._run_ins_func(spider_name, parser_method_name, each_task)
                except Retry_current_task:
                    # handle it like a new task
                    self.inner_ou_q.put(each_task)
                except:
                    traceback.print_exc()

    async def _feed_parser(self):
        while True:
            await self.load_task()

    def feee_parser(self):
        self.loop.create_task(self._feed_parser())
        self.loop.run_forever()

    def run_parser(self):
        while True:
            self.make_tasks()

    def run(self):
        import threading
        # t1 = threading.Thread(target=self.feed_parser)
        t2 = threading.Thread(target=self.run_parser)

        # t1.start()
        t2.start()
