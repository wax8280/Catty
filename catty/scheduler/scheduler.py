#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/24 13:14

import threading
import time
import traceback

from catty.config import CONFIG
from catty.exception import *
from catty.libs.handle_module import SpiderModuleHandle
from catty.message_queue.redis_queue import AsyncRedisPriorityQueue
from catty.scheduler.tasker import Tasker

NOTSTART = 0
STARTED = 1
READY_TO_START = 2
STOP = 3

LOAD_QUEUE_SLEEP = 0.5
SELECTOR_SLEEP = 3


class Scheduler(object):
    LOAD_SPIDER_INTERVAL = 1

    def __init__(self,
                 scheduler_downloader_queue: AsyncRedisPriorityQueue,
                 parser_scheduler_queue: AsyncRedisPriorityQueue,
                 loop):
        self.scheduler_downloader_queue = scheduler_downloader_queue
        self.parser_scheduler_queue = parser_scheduler_queue
        self.requests_queue = {}

        self._stop = False

        self.tasker = Tasker()

        self.spider_stoped = set()
        self.spider_started = set()

        # schema
        # {
        #   'spider_name':{status,}
        # }
        self.loaded_spider = {}

        self.loop = loop
        self.lock = threading.Lock()

        self.spider_module_handle = SpiderModuleHandle(CONFIG['SPIDER_PATH'])
        # self.spider_module_handle.to_instance_spider()

        self.instantiate_spider()

    def instantiate_spider(self):
        """instantiate all spider which had loaded and not instantiated"""
        self.spider_module_handle.to_instance_spider()
        for spider_name in self.spider_module_handle.namespace.keys():
            try:
                spider_name = getattr(self.spider_module_handle.namespace[spider_name][1], 'Spider').name
            except AttributeError:
                continue
            self.loaded_spider.update({spider_name: {'status': READY_TO_START}})

    # ---------------------Thread async queue-----------------
    def select_task(self):
        """base on the exetime select the task to push"""
        all_task = []
        # TODO 暂时没有找到好的遍历队列的方法
        # 全部出队
        try:
            while True:
                all_task.append(self.task_to_downloader.get_nowait())
        except:
            pass

        # 遍历队列
        for each_task in all_task:
            if time.time() - each_task['scheduler']['exetime'] > 0:
                print('[select_task]Select tid:{}'.format(each_task['tid']))
                self.selected_task.put(each_task)
            else:
                # 未到执行时间的，重新翻入队列中
                self.task_to_downloader.put(each_task)
        time.sleep(SELECTOR_SLEEP)

    # --------------------------------------------------------

    async def load_task(self):
        """load item from parser-scheduler queue"""
        try:
            t = await self.parser_scheduler_queue.get()
            print('[load_task]Load item')
            return t
        except AsyncQueueEmpty:
            # print('[load_task]Redis Queue is empty')
            await asyncio.sleep(LOAD_QUEUE_SLEEP, loop=self.loop)
            self.loop.create_task(
                self.load_task()
            )
            return
        except Exception:
            traceback.print_exc()
            await asyncio.sleep(LOAD_QUEUE_SLEEP, loop=self.loop)
            self.loop.create_task(
                self.load_task()
            )
            return

    async def push_task(self, q, item):
        """ push task to scheduler-downloader queue"""

        done = False
        while not done:
            try:
                await q.put(item)
                done = True
            except AsyncQueueFull:
                traceback.print_exc()
                # print('[push_task]Redis queue is full')
                await asyncio.sleep(LOAD_QUEUE_SLEEP, loop=self.loop)
            except Exception as e:
                traceback.print_exc()
                await asyncio.sleep(LOAD_QUEUE_SLEEP, loop=self.loop)

        print('[push_task]Push item')
        return True

    async def _push_task_to_request_queue(self, request, spider_name):
        """make the task from request and append it to self._task_to_downloader"""
        request_q = self.requests_queue.setdefault(
            "{}:requests".format(spider_name),
            AsyncRedisPriorityQueue("{}:requests".format(spider_name), loop=self.loop)
        )
        await request_q.conn()
        await self.push_task(request_q, request)

    async def _run_ins_func(self, spider_name, method_name, item=None):
        """run the spider_ins boned method to return a task,and append it to self._task"""
        try:
            spider_ins = self.spider_module_handle.spider_instantiation[spider_name]
        except IndexError:
            print('No this Spider or had not instance')

        # get the method from instance
        method = spider_ins.__getattribute__(method_name)

        if item:
            func_return_item = method(item=item)
        else:
            # without item param,"start" etc...
            func_return_item = method()

        if isinstance(func_return_item, dict):
            await self._push_task_to_request_queue(func_return_item, spider_name)

        # return how many request mean it make how many task
        elif isinstance(func_return_item, list):
            for each_func_return_item in func_return_item:
                await self._push_task_to_request_queue(each_func_return_item, spider_name)

    async def make_tasks(self):
        """get old task from self._task_from_parser and run ins func to  make new task to append it to self._task"""
        for spider_name, value in self.loaded_spider.items():
            if value['status'] == READY_TO_START:
                # start the spider's start method
                try:
                    print('starting {}'.format(spider_name))
                    await self._run_ins_func(
                        spider_name,
                        'start'
                    )
                    value['status'] = STARTED
                except:
                    # TODO 这里捕获所有用户编写的spider里面的错误
                    traceback.print_exc()

        task = await self.load_task()
        if task:
            callback = task['callback']
            spider_name = task['spider_name']

            for callback_method_name in callback:
                fetcher_method_name = callback_method_name.get('fetcher', None)
                item = task['parser']['item']

                if fetcher_method_name:
                    if not isinstance(fetcher_method_name, list):
                        fetcher_method_name = [fetcher_method_name]

                    # a task can have many fetcher callbacks
                    for each_fetcher_method_name in fetcher_method_name:
                        try:
                            # 这里重新生成一个task，如果用户需要保存上一个task的数据（例如meta）必须自己手动处理
                            print('[make_tasks] {}.{} making task'.format(spider_name, each_fetcher_method_name))
                            await self._run_ins_func(
                                spider_name,
                                each_fetcher_method_name,
                                item
                            )
                        except:
                            # TODO 这里捕获所有用户编写的spider里面的错误
                            traceback.print_exc()
                            # TODO 这里去重

        self.loop.create_task(
            self.make_tasks()
        )

    def run(self):
        self.loop.create_task(self.make_tasks())
        self.loop.run_forever()
