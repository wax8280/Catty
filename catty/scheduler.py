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
from catty.libs.utils import get_default, PriorityDict, md5string, dump_task, load_task


class Scheduler(HandlerMixin):
    def __init__(self,
                 scheduler_downloader_queue: AsyncRedisPriorityQueue,
                 parser_scheduler_queue: AsyncRedisPriorityQueue,
                 loop: asyncio.BaseEventLoop):
        """
        :param scheduler_downloader_queue:      AsyncRedisPriorityQueue     The redis queue
        :param parser_scheduler_queue:          AsyncRedisPriorityQueue     The redis queue
        :param loop:                            asyncio.BaseEventLoop               EventLoop
        """
        super(Scheduler, self).__init__()
        self.name = 'Scheduler'
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

        self.spider_module_handle = SpiderModuleHandle(catty.config.SPIDER_PATH)

        self.logger = Log('Scheduler')
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

            if spider_name not in self.spider_paused | self.spider_ready_start | self.spider_started | self.spider_stopped:
                self.spider_todo.add(spider_name)

    def instantiate_selector(self):
        # Get the speed
        self.selector = Selector(
            {spider_name: get_default(
                self.spider_module_handle.spider_instantiation[spider_name], 'speed', catty.config.SPIDER_DEFAULT['SPEED'])
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

    async def load_persist_request_queue_task(self, spider_name: str):
        """load the persist task & push it to request-queue"""
        tasks = load_task(catty.config.DUMP_PATH, 'request_queue', spider_name)
        self.logger.log_it("[load_task]Load tasks:{}".format(tasks))

        if tasks is not None:
            for each_task in tasks:
                # push each task to request-queue
                await self._push_task_to_request_queue(each_task, each_task['spider_name'])

    async def dump_persist_request_queue_task(self, spider_name: str):
        """load the paused-spider's request queue & dump it"""
        request_q = self.requests_queue_conn.setdefault(
            catty.config.REQUEST_QUEUE_FORMAT.format(spider_name),
            AsyncRedisPriorityQueue(catty.config.REQUEST_QUEUE_FORMAT.format(spider_name), loop=self.loop)
        )
        if not request_q.redis_conn:
            await request_q.conn()
        while await request_q.qsize():
            dump_task(await get_task(request_q), catty.config.DUMP_PATH, 'request_queue', spider_name)

    async def clean_queue(self, spider_name: str):
        """Clean the spider's requests queue"""
        request_q = self.requests_queue_conn.setdefault(
            catty.config.REQUEST_QUEUE_FORMAT.format(spider_name),
            AsyncRedisPriorityQueue(catty.config.REQUEST_QUEUE_FORMAT.format(spider_name), loop=self.loop)
        )
        if not request_q.redis_conn:
            await request_q.conn()
        await request_q.clear()

    async def clean_dupefilter(self, spider_name: str):
        """Clean the spider' DumpFilter queue"""
        bloomfilter = self.bloom_filter.get(spider_name)
        if bloomfilter:
            await bloomfilter.clean()

    async def dump_paused_task(self):
        """Dump the task which spider was paused."""
        for paused_spider_name in self.spider_paused:
            await self.dump_persist_request_queue_task(paused_spider_name)

    def dump_speed(self):
        spider_speed = self.selector.spider_speed
        p = pickle.dumps(spider_speed)
        self.logger.log_it("[dump_speed]{}".format(spider_speed))
        root = os.path.join(catty.config.DUMP_PATH, 'scheduler')
        if not os.path.exists(root):
            os.mkdir(root)
        path = os.path.join(root, 'speed')
        with open(path, 'wb') as f:
            f.write(p)

        return True

    def load_speed(self):
        path = os.path.join(os.path.join(catty.config.DUMP_PATH, 'scheduler'), 'speed')
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
        root = os.path.join(catty.config.DUMP_PATH, 'scheduler')
        if not os.path.exists(root):
            os.mkdir(root)
        path = os.path.join(root, 'status')
        with open(path, 'wb') as f:
            f.write(p)

        return True

    def load_status(self):
        path = os.path.join(os.path.join(catty.config.DUMP_PATH, 'scheduler'), 'status')
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
        self.instantiate_spider()
        self.instantiate_selector()
        self.load_speed()

    async def on_end(self):
        """Run when exit to do something like dump queue etc... It make self.done_all_things=True in last """
        if catty.config.PERSISTENCE['PERSIST_BEFORE_EXIT']:
            for spider_set in self.all_spider_set:
                for spider_name in spider_set:
                    await self.dump_persist_request_queue_task(spider_name)

        self.dump_speed()
        self.dump_status()
        self.done_all_things = True

    async def load_all_task(self):
        for spider_set in self.all_spider_set:
            for spider_name in spider_set:
                await self.dump_persist_request_queue_task(spider_name)

    async def _push_task_to_request_queue(self, request: dict, spider_name: str):
        """push the task to request-queue"""
        request_q = self.requests_queue_conn.setdefault(
            catty.config.REQUEST_QUEUE_FORMAT.format(spider_name),
            AsyncRedisPriorityQueue(catty.config.REQUEST_QUEUE_FORMAT.format(spider_name), loop=self.loop)
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
                if each_func_return_item:
                    task = self.tasker.make(each_func_return_item)

                # DupeFilter
                if task['meta']['catty.config.DUPE_FILTER']:
                    spider_ins = self.spider_module_handle.spider_instantiation.get(spider_name)
                    if not spider_ins:
                        return
                    seeds = get_default(spider_ins, 'seeds', catty.config.DUPE_FILTER['SEEDS'])
                    blocknum = get_default(spider_ins, 'blocknum', catty.config.DUPE_FILTER['BLOCKNUM'])

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
                dump_task(task, catty.config.DUMP_PATH, 'scheduler', task['spider_name'])
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

    def run_scheduler(self):
        self.loop.create_task(self.selector.select_task())
        self.loop.create_task(self.make_tasks())
        self.loop.create_task(self.dump_paused_task())
        self.loop.run_forever()

    def run(self):
        try:
            self.on_begin()
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

    async def select_task(self):
        # TODO 时间粒度
        last_running_time = self.running_time
        self.running_time = int(time.time()) - self.run_at

        # FIXME dump paused spider should not in Selector
        for spider_name in self.spider_started | self.spider_paused:
            speed_reciprocal = self.spider_speed_reciprocal[spider_name]
            for each_diff_time in range(last_running_time, self.running_time):
                # time's up
                if each_diff_time % speed_reciprocal == 0:

                    # if speed bigger than 1,means that at last 1 request per sec.
                    if self.spider_speed[spider_name] > 1:
                        for i in range(self.spider_speed[spider_name]):
                            requests_q = self.requests_queue.get(catty.config.REQUEST_QUEUE_FORMAT.format(spider_name))
                            if requests_q:
                                task = await get_task(requests_q)
                                if task:
                                    if task['spider_name'] in self.spider_started:
                                        await push_task(self.scheduler_downloader_queue, task, self.loop)
                                        self.logger.log_it('[select_task]{} tid:{}'.format(spider_name, task['tid']))
                                    elif task['spider_name'] in self.spider_paused:
                                        dump_task(task, catty.config.DUMP_PATH, 'scheduler', task['spider_name'])
                                    elif task['spider_name'] in self.spider_stopped:
                                        pass
                    else:
                        requests_q = self.requests_queue.get(catty.config.REQUEST_QUEUE_FORMAT.format(spider_name))
                        if requests_q:
                            task = await get_task(requests_q)
                            if task:
                                if task['spider_name'] in self.spider_started:
                                    await push_task(self.scheduler_downloader_queue, task, self.loop)
                                    self.logger.log_it('[select_task]{} tid:{}'.format(spider_name, task['tid']))
                                elif task['spider_name'] in self.spider_paused:
                                    dump_task(task, catty.config.DUMP_PATH, 'scheduler', task['spider_name'])
                                elif task['spider_name'] in self.spider_stopped:
                                    pass

        await asyncio.sleep(catty.config.SELECTOR_INTERVAL, loop=self.loop)
        self.loop.create_task(self.select_task())


class Tasker(object):
    """
    {
    'task': {
        'tid': str,                         md5(request.url + request.data)
        'spider_name': str,                 Spider name
        'priority': int,                    Priority of task
        'retried': int,                     Retried count
        'meta':dict                         A dict to some config or something to save
        {
            'retry': int                    The count of retry.Default: 0
            'retry_wait': int               Default: 3
            'catty.config.DUPE_FILTER': bool             Default: False
        },

        'request': Request_obj
        {
            'method':str                    HTTP method
            'url':str                       URL
            'params':dict/str               string must be encoded(optional)
            'data':dict/bytes               to send in the body of the request(optional)
            'headers':dict                  HTTP Headers(optional)
            'auth':aiohttp.BasicAuth        an object that represents HTTP Basic Authorization (optional)
            'allow_redirects':bool
            'proxy':str                     Proxy URL(optional)
            'proxy_auth':aiohttp.BasicAuth  an object that represents proxy HTTP Basic Authorization (optional)
            'timeout':int                   a timeout for IO operations, 5min by default(option).Use None or 0 to disable timeout checks.
        }

        'downloader': {
        },

        'scheduler': {
        },

        'parser': {
            'item': dict,          # the dict return from parser func
        },

        'response': Response_obj,
        {
            'status':int                    HTTP status code
            'method':str                    HTTP method
            'cookies':SimpleCookie          HTTP cookies of response (Set-Cookie HTTP header)
            'headers':list                  HTTP headers of response as unconverted bytes, a sequence of (key, value) pairs.
            'content_type':str              Content-Type header
            'charset':str                   The value is parsed from the Content-Type HTTP header.Returns str like 'utf-8' or None if no Content-Type header present in HTTP headers or it has no charset information.
            'body':bytes                    response’s body as bytes.
            'use_time':float                the time cost in request
        }

        'callback': list,      # bound method      {'fetcher':bound_method,'parser':bound_method,'result_pipeline':'bound_method}
    }

    }
    """

    def _make_task(self, request):
        spider_name = request['spider_name']
        priority = request['priority']
        callback = request['callback']
        meta = request['meta']

        tid = md5string(request['request']['url'] + str(request['request']['data']))

        return PriorityDict({
            'tid': tid,
            'spider_name': spider_name,
            'priority': priority,
            'meta': meta,
            'request': request['request'],
            'downloader': {},
            'scheduler': {},
            'parser': {},
            'response': {},
            'callback': callback,
        })

    def make(self, request):
        return self._make_task(request)

    def dump_task(self, task):
        return pickle.dumps(task)

    def load_task(self, dumped_task):
        return pickle.loads(dumped_task)
