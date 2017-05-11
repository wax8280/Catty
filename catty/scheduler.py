#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/24 13:14
import asyncio
import math
import os
import pickle
import threading
import time
import traceback

import catty.config
from catty.message_queue import AsyncRedisPriorityQueue, get_task, push_task
from catty.handler import HandlerMixin
from catty.libs.bloom_filter import RedisBloomFilter
from catty.libs.handle_module import SpiderModuleHandle
from catty.libs.log import Log
from catty.libs.utils import get_default, Task, dump_task, load_task


class Scheduler(HandlerMixin):
    def __init__(self,
                 scheduler_downloader_queue: AsyncRedisPriorityQueue,
                 parser_scheduler_queue: AsyncRedisPriorityQueue,
                 loop: asyncio.BaseEventLoop):
        """
        :param scheduler_downloader_queue:The redis queue
        :param parser_scheduler_queue:The redis queue
        :param loop:EventLoop
        """
        super().__init__()
        self.name = 'Scheduler'
        self.scheduler_downloader_queue = scheduler_downloader_queue
        self.parser_scheduler_queue = parser_scheduler_queue
        # connection of all requests-queue
        self.requests_queue_conn = {}
        self.bloom_filter = {}
        self.loop = loop

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

        self.spider_module_handle = SpiderModuleHandle(catty.config.SPIDER_PATH)

        self.logger = Log('Scheduler')
        self.done_all_things = False
        self.selector = None

    def instantiate_spider(self):
        """instantiate all spider"""
        self.spider_module_handle.load_all_spider()
        for spider_name in self.spider_module_handle.namespace.keys():
            try:
                # find Spider.name
                spider_name = getattr(self.spider_module_handle.namespace[spider_name][1], 'Spider').name
            except AttributeError:
                continue

            if spider_name not \
                    in (self.spider_paused | self.spider_ready_start | self.spider_started | self.spider_stopped):
                self.spider_todo.add(spider_name)

    def instantiate_selector(self):
        # Get the speed
        self.selector = Selector(
            {spider_name: get_default(
                self.spider_module_handle.spider_instantiation[spider_name],
                'speed',
                catty.config.SPIDER_DEFAULT['SPEED'])
             for spider_set in self.all_spider_set for spider_name in spider_set},
            self.scheduler_downloader_queue,
            self.requests_queue_conn,
            self.spider_stopped,
            self.spider_paused,
            self.spider_started,
            self.spider_ready_start,
            self.spider_todo,
            self.loop,
        )

    async def load_persist_task(self, spider_name: str):
        """load the persist task & push it to request-queue"""
        tasks = load_task(catty.config.PERSISTENCE['DUMP_PATH'], 'request_queue', spider_name)
        self.logger.log_it("[load_task]Load tasks:{}".format(tasks))

        if tasks is not None:
            for each_task in tasks:
                # push each task to request-queue
                request_q = self.requests_queue_conn.setdefault(
                    "{}:requests".format(spider_name),
                    AsyncRedisPriorityQueue("{}:requests".format(spider_name), loop=self.loop)
                )
                await push_task(request_q, each_task, self.loop)

    async def dump_persist_task(self, spider_name: str):
        """load the request-queue & dump it"""
        request_q = self.requests_queue_conn.setdefault(
            "{}:requests".format(spider_name),
            AsyncRedisPriorityQueue("{}:requests".format(spider_name), loop=self.loop)
        )
        if not request_q.redis_conn:
            await request_q.conn()
        while await request_q.qsize():
            dump_task(await get_task(request_q), catty.config.PERSISTENCE['DUMP_PATH'], 'request_queue', spider_name)

    async def clean_queue(self, spider_name: str):
        """Clean the spider's requests queue"""
        request_q = self.requests_queue_conn.setdefault(
            "{}:requests".format(spider_name),
            AsyncRedisPriorityQueue("{}:requests".format(spider_name), loop=self.loop)
        )
        if not request_q.redis_conn:
            await request_q.conn()
        await request_q.clear()

    async def get_requests_queue_size(self, spider_name: str):
        request_q = self.requests_queue_conn.setdefault(
            "{}:requests".format(spider_name),
            AsyncRedisPriorityQueue("{}:requests".format(spider_name), loop=self.loop)
        )
        await request_q.qsize()

    async def clean_dupefilter(self, spider_name: str):
        """Clean the spider' DumpFilter queue"""
        bloomfilter = self.bloom_filter.get(spider_name)
        if bloomfilter:
            await bloomfilter.conn()
            await bloomfilter.clean()

    async def dump_all_paused_task(self):
        """Dump the task which spider was paused."""
        for paused_spider_name in self.spider_paused:
            await self.dump_persist_task(paused_spider_name)

    def dump_speed(self):
        p = pickle.dumps(self.selector.spider_speed)
        self.logger.log_it("[dump_speed]{}".format(self.selector.spider_speed))
        root = os.path.join(catty.config.PERSISTENCE['DUMP_PATH'], 'scheduler')
        if not os.path.exists(root):
            os.mkdir(root)
        path = os.path.join(root, 'speed')
        with open(path, 'wb') as f:
            f.write(p)

    def load_speed(self):
        path = os.path.join(os.path.join(catty.config.PERSISTENCE['DUMP_PATH'], 'scheduler'), 'speed')
        if os.path.exists(path):
            with open(path, 'rb') as f:
                t = f.read()

            spider_speed = pickle.loads(t)
            self.logger.log_it("[load_speed]{}".format(spider_speed))
            self.selector.spider_speed = spider_speed
            self.selector.init_speed()

    def dump_status(self):
        status = {
            'spider_started': self.spider_started,
            'spider_paused': self.spider_paused,
            'spider_stopped': self.spider_stopped,
            'spider_todo': self.spider_todo,
            'spider_ready_start': self.spider_ready_start,
        }
        p = pickle.dumps(status)
        self.logger.log_it("[dump_status]{}".format(status))
        root = os.path.join(catty.config.PERSISTENCE['DUMP_PATH'], 'scheduler')
        if not os.path.exists(root):
            os.mkdir(root)
        path = os.path.join(root, 'status')
        with open(path, 'wb') as f:
            f.write(p)

    def load_status(self):
        path = os.path.join(os.path.join(catty.config.PERSISTENCE['DUMP_PATH'], 'scheduler'), 'status')
        if os.path.exists(path):
            with open(path, 'rb') as f:
                t = f.read()

            status = pickle.loads(t)
            self.logger.log_it("[load_status]{}".format(status))
            self.spider_paused = status['spider_paused']
            self.spider_stopped = status['spider_stopped']
            self.spider_started = status['spider_started']
            self.spider_todo = status['spider_todo']
            self.spider_ready_start = status['spider_ready_start']
            self.all_spider_set = [
                self.spider_todo, self.spider_paused, self.spider_started, self.spider_ready_start, self.spider_started
            ]

    def on_begin(self):
        """Run before begin to do something like load tasks,or load config"""
        self.load_status()
        for started_spider in self.spider_started:
            # load every started_spider's requests
            self.loop.create_task(self.load_persist_task(started_spider))
        self.instantiate_spider()
        self.instantiate_selector()
        self.load_speed()

    async def check_end(self):
        done = []
        while not done or False in done:
            done = []
            for spider_set in self.all_spider_set:
                for spider_name in spider_set:
                    q = await self.get_requests_queue_size(spider_name)
                    if q == 0 or q is None:
                        await asyncio.sleep(0.5, self.loop)
                        done.append(True)
                    else:
                        done.append(False)

        self.done_all_things = True

    async def on_end(self):
        """Run when exit to do something like dump queue etc... It make self.done_all_things=True in last """
        for task in asyncio.Task.all_tasks():
            task.cancel()

        if catty.config.PERSISTENCE['PERSIST_BEFORE_EXIT']:
            for spider_set in self.all_spider_set:
                for spider_name in spider_set:
                    self.loop.create_task(self.dump_persist_task(spider_name))

        self.dump_speed()
        self.dump_status()

        self.loop.create_task(self.check_end())

    async def push_requests(self, task, spider_ins, spider_name):
        # DupeFilter
        f = True
        if task['meta']['dupe_filter']:
            seeds = get_default(spider_ins, 'seeds', catty.config.SPIDER_DEFAULT['SEEDS'])
            blocknum = get_default(spider_ins, 'blocknum', catty.config.SPIDER_DEFAULT['BLOCKNUM'])
            bloom_filter = self.bloom_filter.setdefault(
                task['spider_name'],
                RedisBloomFilter(self.loop, task['spider_name'] + ':DupeFilter', seeds, blockNum=blocknum)
            )

            if not bloom_filter.redis_conn:
                await bloom_filter.conn()

            if not await bloom_filter.is_contain(task['tid']):
                await bloom_filter.add(task['tid'])
            else:
                self.logger.log_it("[run_ins_func]Filtered tid:{} url:{} data:{} params:{}".format(
                    task['tid'],
                    task['request'].url,
                    task['request'].data,
                    task['request'].params
                ))
                f = False

        if f:
            self.logger.log_it("[run_ins_func]New request tid:{} url:{} data:{} params:{}".format(
                task['tid'],
                task['request'].url,
                task['request'].data,
                task['request'].params
            ))
            request_q = self.requests_queue_conn.setdefault(
                "{}:requests".format(spider_name),
                AsyncRedisPriorityQueue("{}:requests".format(spider_name), loop=self.loop)
            )
            await push_task(request_q, task, self.loop)

    async def _run_ins_func(self, spider_name: str, method_name: str, task: dict = None):
        """run the spider_ins boned method to return a task & push it to request-queue"""
        try:
            spider_ins = self.spider_module_handle.spider_instantiation[spider_name]
        except IndexError:
            self.logger.log_it('[run_ins_func]No this Spider or had not instance', 'WARN')
            # try to reload it
            self.spider_module_handle.update_spider(spider_name)
            return

        # get the method from instance
        method = spider_ins.__getattribute__(method_name)

        try:
            if task:
                func_return_task = method(task=task)
            else:
                # without task param,"start" etc...
                func_return_task = method()
        except:
            # The except from user spiders
            traceback.print_exc()
            return

        if not isinstance(func_return_task, list):
            func_return_task = [func_return_task]

        # return how many request mean it make how many task
        if isinstance(func_return_task, list):
            for each_task in func_return_task:
                if not isinstance(each_task, Task):
                    self.logger.log_it("[run_ins_func]Not return a Task in {}".format(spider_name), 'WARN')
                    continue
                self.loop.create_task(self.push_requests(each_task, spider_ins, spider_name))

    async def make_tasks(self):
        """run the ready_start spider & run the done task & push them"""
        # start the "ready_start" spiders
        had_started_ = set()
        for spider_name in self.spider_ready_start:
            # start the spider's start method
            self.logger.log_it('[make_tasks]Starting spider:{}'.format(spider_name), 'INFO')
            self.loop.create_task(self._run_ins_func(spider_name, 'start'))
            self.spider_started.add(spider_name)
            had_started_.add(spider_name)

        self.spider_ready_start -= had_started_

        # from done task
        task = await get_task(self.parser_scheduler_queue)
        if task:
            self.loop.create_task(self.make_tasks())
            spider_name = task['spider_name']

            if task['spider_name'] in self.spider_started:
                callback = task['callback']

                for callback_method_name in callback:
                    fetcher_method_name = callback_method_name.get('fetcher', None)

                    if not fetcher_method_name:
                        continue

                    if not isinstance(fetcher_method_name, list):
                        fetcher_method_name = [fetcher_method_name]

                    # a task can have many fetcher callbacks
                    for each_fetcher_method_name in fetcher_method_name:
                        # make a new task,if use need to save the data from last task(meta etc..),must handle it.
                        self.logger.log_it(
                            '[make_tasks]{}.{} making task'.format(spider_name, each_fetcher_method_name))
                        self.loop.create_task(self._run_ins_func(spider_name, each_fetcher_method_name, task))

            elif task['spider_name'] in self.spider_paused:
                # persist
                dump_task(task, catty.config.PERSISTENCE['DUMP_PATH'], 'scheduler', task['spider_name'])
                self.loop.create_task(self.dump_persist_task(spider_name))
            elif task['spider_name'] in self.spider_stopped:
                pass
            elif task['spider_name'] in self.spider_todo:
                # FIXME if a load the dump data,scheduler will drop it because it is not started.
                pass
        else:
            await asyncio.sleep(catty.config.LOAD_QUEUE_INTERVAL, loop=self.loop)
            self.loop.create_task(
                self.make_tasks()
            )

    def quit(self):
        self.logger.log_it("[Ending]Doing the last thing...")
        self.loop.create_task(self.on_end())
        while True:
            if self.done_all_things:
                self.logger.log_it("Bye!")
                os._exit(0)
            else:
                # doesn't block the thread
                time.sleep(1)

    def run_scheduler(self):
        self.loop.create_task(self.selector.select_task())
        for i in range(catty.config.NUM_OF_SCHEDULER_MAKE_TASK):
            self.loop.create_task(self.make_tasks())
        self.loop.run_forever()

    def run(self):
        try:
            self.on_begin()
            handler_server_thread = threading.Thread(target=self.run_handler)
            handler_server_thread.start()
            scheduler_thread = threading.Thread(target=self.run_scheduler)
            scheduler_thread.start()
            # scheduler_thread.join()
            while True:
                r = input()
                if r == 'Q':
                    self.quit()
        except KeyboardInterrupt:
            self.quit()


class Selector:
    def __init__(self,
                 spider_speed: dict,
                 scheduler_downloader_queue: AsyncRedisPriorityQueue,
                 requests_queue: dict,
                 spider_stopped: set,
                 spider_paused: set,
                 spider_started: set,
                 spider_ready_start: set,
                 spider_todo: set,
                 loop: asyncio.BaseEventLoop):
        self.logger = Log('Selector')
        self.scheduler_downloader_queue = scheduler_downloader_queue
        self.requests_queue = requests_queue
        self.loop = loop

        # time from begin to now
        self.running_time = 0
        self.run_at = int(time.time())

        self.spider_stopped = spider_stopped
        self.spider_paused = spider_paused
        self.spider_started = spider_started
        self.spider_ready_start = spider_ready_start
        self.spider_todo = spider_todo
        self.spider_speed = spider_speed
        self.spider_speed_reciprocal = {}

        self.init_speed()

    def init_speed(self):
        """init all spider speed"""
        self.spider_speed_reciprocal = {k: math.ceil(1 / v) for k, v in self.spider_speed.items()}

    def update_speed(self, spider_name: str, speed: int):
        """update a spider speed"""
        self.spider_speed.update({spider_name: speed})
        self.spider_speed_reciprocal.update({spider_name: math.ceil(1 / speed)})

    async def _select_task(self, requests_q, spider_name):
        task = await get_task(requests_q)
        if task:
            if task['spider_name'] in self.spider_started:
                await push_task(self.scheduler_downloader_queue, task, self.loop)
                self.logger.log_it('[select_task]{} tid:{}'.format(spider_name, task['tid']))
            elif task['spider_name'] in self.spider_paused:
                dump_task(task, catty.config.PERSISTENCE['DUMP_PATH'], 'scheduler',
                          task['spider_name'])
            elif task['spider_name'] in self.spider_stopped:
                pass

    async def select_task(self):
        # TODO 时间粒度
        last_running_time = self.running_time
        self.running_time = int(time.time()) - self.run_at

        for spider_name in self.spider_started:
            speed_reciprocal = self.spider_speed_reciprocal[spider_name]
            requests_q = self.requests_queue.setdefault(
                "{}:requests".format(spider_name),
                AsyncRedisPriorityQueue("{}:requests".format(spider_name), loop=self.loop)
            )
            for each_diff_time in range(last_running_time, self.running_time):
                # time's up
                if each_diff_time % speed_reciprocal == 0:
                    # if speed bigger than 1,means that at last 1 request per sec.
                    if self.spider_speed[spider_name] > 1:
                        for i in range(self.spider_speed[spider_name]):
                            self.loop.create_task(self._select_task(requests_q, spider_name))
                    else:
                        self.loop.create_task(self._select_task(requests_q, spider_name))

        await asyncio.sleep(catty.config.SELECTOR_INTERVAL, loop=self.loop)
        self.loop.create_task(self.select_task())
