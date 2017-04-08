#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/24 13:14

import threading
import traceback

from catty.config import CONFIG
from catty.exception import *
from catty.handler import HandlerMixin
from catty.libs.handle_module import SpiderModuleHandle
from catty.libs.utils import get_default
from catty.message_queue.redis_queue import AsyncRedisPriorityQueue
from catty.scheduler.selector import Selector
from catty.scheduler.tasker import Tasker

NOTSTART = 0
STARTED = 1
READY_TO_START = 2
STOP = 3
PAUSE = 4

LOAD_QUEUE_SLEEP = 0.5
SELECTOR_SLEEP = 3
REQUEST_QUEUE_FORMAT = "{}:requests"


class Scheduler(HandlerMixin):
    LOAD_SPIDER_INTERVAL = 1

    def __init__(self,
                 scheduler_downloader_queue: AsyncRedisPriorityQueue,
                 parser_scheduler_queue: AsyncRedisPriorityQueue,
                 loop):
        super(Scheduler, self).__init__('SCHEDULER')
        self.scheduler_downloader_queue = scheduler_downloader_queue
        self.parser_scheduler_queue = parser_scheduler_queue
        self.requests_queue = {}

        self._stop = False

        self.tasker = Tasker()

        self.spider_stopped = set()
        self.spider_paused = set()
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

        self.selector = Selector(
            {spider_name: get_default(self.spider_module_handle.spider_instantiation[spider_name], 'speed', 1)
             for spider_name in self.loaded_spider.keys()},
            scheduler_downloader_queue,
            self.requests_queue,
            self.loop
        )

    def instantiate_spider(self):
        """instantiate all spider which had loaded and not instantiated"""
        self.spider_module_handle.to_instance_spider()
        for spider_name in self.spider_module_handle.namespace.keys():
            try:
                spider_name = getattr(self.spider_module_handle.namespace[spider_name][1], 'Spider').name
            except AttributeError:
                continue
            self.loaded_spider.update({spider_name: {'status': READY_TO_START}})

    async def load_task(self, q):
        """load item from parser-scheduler queue"""
        try:
            t = await q.get()
            print('[load_task]Load item from {}'.format(q))
            return t
        except AsyncQueueEmpty:
            # print('[load_task]Redis Queue is empty')
            await asyncio.sleep(LOAD_QUEUE_SLEEP, loop=self.loop)
            self.loop.create_task(
                self.load_task(q)
            )
            return
        except Exception:
            traceback.print_exc()
            await asyncio.sleep(LOAD_QUEUE_SLEEP, loop=self.loop)
            self.loop.create_task(
                self.load_task(q)
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
            REQUEST_QUEUE_FORMAT.format(spider_name),
            AsyncRedisPriorityQueue(REQUEST_QUEUE_FORMAT.format(spider_name), loop=self.loop)
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
                    self.spider_started.add(spider_name)
                except:
                    # TODO 这里捕获所有用户编写的spider里面的错误
                    traceback.print_exc()

        task = await self.load_task(self.parser_scheduler_queue)
        if task:
            if task['spider_name'] in self.spider_started:
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
            elif task['spider_name'] in self.spider_paused:
                # 持久化
                pass
            elif task['spider_name'] in self.spider_stopped:
                # 抛弃
                pass

        print(self.spider_paused)
        self.loop.create_task(
            self.make_tasks()
        )

    def run_scheduler(self):
        self.loop.create_task(self.selector.select_task())
        self.loop.create_task(self.make_tasks())
        self.loop.run_forever()

    def run(self):
        import threading
        handler_server_thread = threading.Thread(target=self.run_handler)
        handler_server_thread.start()
        scheduler_thread = threading.Thread(target=self.run_scheduler)
        scheduler_thread.start()

