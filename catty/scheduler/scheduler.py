#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/24 13:14

import threading
import traceback
import time
from asyncio import BaseEventLoop

from catty.config import CONFIG
from catty.exception import *
from catty.handler import HandlerMixin
from catty.libs.handle_module import SpiderModuleHandle
from catty.libs.utils import *
from catty.libs.bloom_filter import RedisBloomFilter
from catty.message_queue.redis_queue import AsyncRedisPriorityQueue, get_task, push_task
from catty.scheduler.selector import Selector
from catty.scheduler.tasker import Tasker
from catty import SCHEDULER, REQUEST_QUEUE_FORMAT, LOAD_QUEUE_SLEEP, SPIDER_DEFAULT
from catty.libs.log import Log


class Scheduler(HandlerMixin):
    def __init__(self,
                 scheduler_downloader_queue: AsyncRedisPriorityQueue,
                 parser_scheduler_queue: AsyncRedisPriorityQueue,
                 loop: BaseEventLoop):
        """
        :param scheduler_downloader_queue:      AsyncRedisPriorityQueue     The redis queue
        :param parser_scheduler_queue:          AsyncRedisPriorityQueue     The redis queue
        :param loop:                            BaseEventLoop               EventLoop
        """
        super(Scheduler, self).__init__()
        self.name = SCHEDULER
        self.scheduler_downloader_queue = scheduler_downloader_queue
        self.parser_scheduler_queue = parser_scheduler_queue
        # connection of all requests-queue
        self.requests_queue_conn = {}
        self.bloom_filter = {}
        self.loop = loop

        self.tasker = Tasker()

        # spider_ready_start: means that you start a spider,it will run from begin.
        # spider_new: means that you spider that had not run yet.
        self.spider_stopped = set()
        self.spider_paused = set()
        self.spider_started = set()
        self.spider_ready_start = set()
        self.spider_todo = set()

        self.all_spider_set = [
            self.spider_todo, self.spider_paused, self.spider_started, self.spider_ready_start, self.spider_started
        ]

        self.spider_module_handle = SpiderModuleHandle(CONFIG['SPIDER_PATH'])
        self.instantiate_spider()

        # Get the speed
        self.spider_speed = {spider_name: get_default(
            self.spider_module_handle.spider_instantiation[spider_name], 'SPEED', SPIDER_DEFAULT['SPEED'])
                             for spider_set in self.all_spider_set for spider_name in spider_set}

        self.selector = Selector(
            self.spider_speed,
            scheduler_downloader_queue,
            self.requests_queue_conn,
            self.spider_stopped,
            self.spider_paused,
            self.spider_started,
            self.spider_ready_start,
            self.spider_todo,
            self.loop,
        )

        self.logger = Log(SCHEDULER)
        self.done_all_things = False



    def instantiate_spider(self):
        """instantiate all spider"""
        self.spider_module_handle.load_all_spider()
        for spider_name in self.spider_module_handle.namespace.keys():
            try:
                # find Spider.name
                spider_name = getattr(self.spider_module_handle.namespace[spider_name][1], 'Spider').name
            except AttributeError:
                continue
            self.spider_todo.add(spider_name)

    async def load_persist_request_queue_task(self, spider_name: str):
        """load the persist task & push it to request-queue"""
        tasks = load_task(CONFIG['DUMP_PATH'], 'request_queue', spider_name)
        self.logger.log_it("[load_task]Load tasks:{}".format(tasks))

        if tasks is not None:
            for each_task in tasks:
                # push each task to request-queue
                await self._push_task_to_request_queue(each_task, each_task['spider_name'])

    async def dump_persist_request_queue_task(self, spider_name: str):
        """load the paused-spider's request queue & dump it"""
        request_q = self.requests_queue_conn.setdefault(
            REQUEST_QUEUE_FORMAT.format(spider_name),
            AsyncRedisPriorityQueue(REQUEST_QUEUE_FORMAT.format(spider_name), loop=self.loop)
        )
        if not request_q.redis_conn:
            await request_q.conn()
        while await request_q.qsize():
            dump_task(await get_task(request_q), CONFIG['DUMP_PATH'], 'request_queue', spider_name)

    async def clean_queue(self, spider_name: str):
        """Clean the spider's requests queue"""
        request_q = self.requests_queue_conn.setdefault(
            REQUEST_QUEUE_FORMAT.format(spider_name),
            AsyncRedisPriorityQueue(REQUEST_QUEUE_FORMAT.format(spider_name), loop=self.loop)
        )
        if not request_q.redis_conn:
            await request_q.conn()
        await request_q.clear()

    async def clean_dupefilter(self, spider_name: str):
        """Clean the spider' DumpFilter queue"""
        pass

    async def dump_paused_task(self):
        """Dump the task which spider was paused."""
        for paused_spider_name in self.spider_paused:
            await self.dump_persist_request_queue_task(paused_spider_name)

    async def on_end(self):
        if CONFIG['PERSISTENCE']['PERSIST_BEFORE_EXIT']:
            for spider_set in self.all_spider_set:
                for spider_name in spider_set:
                    await self.dump_persist_request_queue_task(spider_name)
        self.done_all_things = True

    async def load_all_task(self):
        for spider_set in self.all_spider_set:
            for spider_name in spider_set:
                await self.dump_persist_request_queue_task(spider_name)

    async def _push_task_to_request_queue(self, request: dict, spider_name: str):
        """push the task to request-queue"""
        request_q = self.requests_queue_conn.setdefault(
            REQUEST_QUEUE_FORMAT.format(spider_name),
            AsyncRedisPriorityQueue(REQUEST_QUEUE_FORMAT.format(spider_name), loop=self.loop)
        )
        await push_task(request_q, request, self.loop)

    async def _run_ins_func(self, spider_name: str, method_name: str, task: dict = None):
        """run the spider_ins boned method to return a task & push it to request-queue"""
        try:
            spider_ins = self.spider_module_handle.spider_instantiation[spider_name]
        except IndexError:
            self.logger.log_it('[run_ins_func]No this Spider or had not instance', 'WARN')
            return

        # get the method from instance
        method = spider_ins.__getattribute__(method_name)

        if task:
            func_return_task = method(task=task)
        else:
            # without task param,"start" etc...
            func_return_task = method()

        if isinstance(func_return_task, dict):
            func_return_task = [func_return_task]

        # return how many request mean it make how many task
        if isinstance(func_return_task, list):
            for each_func_return_item in func_return_task:
                f = True
                task = self.tasker.make(each_func_return_item)

                # DupeFilter
                if task['meta']['dupe_filter']:
                    spider_ins = self.spider_module_handle.spider_instantiation.get(spider_name)
                    if not spider_ins:
                        return
                    seeds = get_default(spider_ins, 'seeds', CONFIG['DUPE_FILTER']['SEEDS'])
                    blocknum = get_default(spider_ins, 'blocknum', CONFIG['DUPE_FILTER']['BLOCKNUM'])

                    bloom_filter = self.bloom_filter.setdefault(
                        task['spider_name'],
                        RedisBloomFilter(self.loop, task['spider_name'] + ':DupeFilter', seeds, blockNum=blocknum)
                    )
                    if not bloom_filter.redis_conn:
                        await bloom_filter.conn()

                    if not await bloom_filter.is_contain(task['tid']):
                        await bloom_filter.add(task['tid'])
                    else:
                        self.logger.log_it("[run_ins_func]Filtered tid:{} url:{} data:{}".format(
                            task['tid'],
                            task['request'].url,
                            task['request'].data))
                        f = False

                if f:
                    self.logger.log_it("[run_ins_func]New request tid:{} url:{} data:{}".format(
                        task['tid'],
                        task['request'].url,
                        task['request'].data))
                    await self._push_task_to_request_queue(task, spider_name)

    async def make_tasks(self):
        """run the ready_start spider & run the done task & push them"""

        # start the "ready_start" spiders
        had_started_ = set()
        for spider_name in self.spider_ready_start:
            # start the spider's start method
            try:
                self.logger.log_it('[make_tasks]Starting spider:{}'.format(spider_name), 'INFO')
                await self._run_ins_func(
                    spider_name,
                    'start'
                )
                self.spider_started.add(spider_name)
                had_started_.add(spider_name)
            except:
                # The except from user spiders
                traceback.print_exc()
        self.spider_ready_start -= had_started_

        # from done task
        task = await get_task(self.parser_scheduler_queue)
        if task:
            self.loop.create_task(
                self.make_tasks()
            )
            if task['spider_name'] in self.spider_started:
                callback = task['callback']
                spider_name = task['spider_name']

                for callback_method_name in callback:
                    fetcher_method_name = callback_method_name.get('fetcher', None)

                    if not fetcher_method_name:
                        continue

                    if not isinstance(fetcher_method_name, list):
                        fetcher_method_name = [fetcher_method_name]

                    # a task can have many fetcher callbacks
                    for each_fetcher_method_name in fetcher_method_name:
                        try:
                            # make a new task,if use need to save the data from last task(meta etc..),must handle it.
                            self.logger.log_it(
                                '[make_tasks]{}.{} making task'.format(spider_name, each_fetcher_method_name))
                            await self._run_ins_func(
                                spider_name,
                                each_fetcher_method_name,
                                task
                            )
                        except:
                            # The except from user spiders
                            traceback.print_exc()

            elif task['spider_name'] in self.spider_paused:
                # persist
                dump_task(task, CONFIG['DUMP_PATH'], 'scheduler', task['spider_name'])
            elif task['spider_name'] in self.spider_stopped:
                pass
            elif task['spider_name'] in self.spider_todo:
                # FIXME if a load the dump data,scheduler will drop it because it is not started.
                pass
        else:
            await asyncio.sleep(LOAD_QUEUE_SLEEP, loop=self.loop)

            self.loop.create_task(
                self.make_tasks()
            )

    def run_scheduler(self):
        self.loop.create_task(self.selector.select_task())
        self.loop.create_task(self.make_tasks())
        self.loop.create_task(self.dump_paused_task())
        self.loop.run_forever()

    def run(self):
        try:
            handler_server_thread = threading.Thread(target=self.run_handler)
            handler_server_thread.start()
            scheduler_thread = threading.Thread(target=self.run_scheduler)
            scheduler_thread.start()
            scheduler_thread.join()
        except KeyboardInterrupt:
            self.logger.log_it("[Ending]Doing the last thing...")
            self.loop.create_task(self.on_end())
            while not self.done_all_things:
                time.sleep(1)
            self.logger.log_it("Bye!")
            os._exit(0)
