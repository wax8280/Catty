#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/26 10:05
import asyncio
import inspect
import os
import pickle
import threading
import time
import traceback
from asyncio import BaseEventLoop
from copy import deepcopy

import catty.config
from catty.message_queue import AsyncRedisPriorityQueue, get_task, push_task
from catty.handler import HandlerMixin
from catty.config import LOAD_QUEUE_INTERVAL
from catty.exception import Retry_current_task
from catty.libs.count import Counter
from catty.libs.handle_module import SpiderModuleHandle
from catty.libs.log import Log
from catty.libs.utils import dump_task, load_task


class Parser(HandlerMixin):
    def __init__(self,
                 downloader_parser_queue: AsyncRedisPriorityQueue,
                 parser_scheduler_queue: AsyncRedisPriorityQueue,
                 loop: BaseEventLoop):
        """
        :param downloader_parser_queue:         AsyncRedisPriorityQueue     The redis queue
        :param parser_scheduler_queue:          AsyncRedisPriorityQueue     The redis queue
        :param loop:                            BaseEventLoop               EventLoop
        """
        super(Parser, self).__init__()
        self.name = 'Parser'

        self.downloader_parser_queue = downloader_parser_queue
        self.parser_scheduler_queue = parser_scheduler_queue
        self.loop = loop

        self.spider_started = set()
        self.spider_stopped = set()
        self.spider_paused = set()

        self.all_spider_set = [self.spider_started, self.spider_stopped, self.spider_paused]

        self.spider_module_handle = SpiderModuleHandle(catty.config.SPIDER_PATH)
        self.spider_module_handle.load_all_spider()

        self.logger = Log('')
        self.done_all_things = False
        self.counter = Counter(loop)

    async def conn_redis(self):
        """Conn the redis"""
        await self.downloader_parser_queue.conn()
        await self.parser_scheduler_queue.conn()

    async def load_persist_task(self):
        """load the persist task & push it to request-queue"""
        # TODO dump downloader-parser queue
        # load the parser_scheduler dumped files.
        tasks = [load_task(catty.config.DUMP_PATH, 'parser_scheduler', spider_name) for spider_name in self.spider_started]
        self.logger.log_it("[load_task]Load tasks:{}".format(tasks))

        if tasks and tasks[0] is not None:
            for spider_tasks in tasks:
                for each_task in spider_tasks:
                    # push each task to parser-scheduler queue
                    await push_task(self.parser_scheduler_queue, each_task, self.loop)

    def dump_count(self):
        counter_date = {'value_d': self.counter.value_d,
                        'cache_value': self.counter.cache_value}
        p = pickle.dumps(counter_date)
        root = os.path.join(catty.config.DUMP_PATH, 'parser')
        if not os.path.exists(root):
            os.mkdir(root)
        path = os.path.join(root, 'counter')
        with open(path, 'wb') as f:
            f.write(p)
        self.logger.log_it("[load_count]{}".format(counter_date))
        return True

    def load_count(self):
        path = os.path.join(os.path.join(catty.config.DUMP_PATH, 'parser'), 'counter')
        if os.path.exists(path):
            with open(path, 'rb') as f:
                t = f.read()

            counter_date = pickle.loads(t)
            self.counter.value_d = counter_date['value_d']
            self.counter.cache_value = counter_date['cache_value']
            self.logger.log_it("[load_count]{}".format(counter_date))

    def dump_status(self):
        status = {
            'spider_started': self.spider_started,
            'spider_paused': self.spider_paused,
            'spider_stopped': self.spider_stopped
        }
        p = pickle.dumps(status)
        root = os.path.join(catty.config.DUMP_PATH, 'parser')
        if not os.path.exists(root):
            os.mkdir(root)
        path = os.path.join(root, 'status')
        with open(path, 'wb') as f:
            f.write(p)
        self.logger.log_it("[dump_status]{}".format(status))
        return True

    def load_status(self):
        path = os.path.join(os.path.join(catty.config.DUMP_PATH, 'parser'), 'status')
        if os.path.exists(path):
            with open(path, 'rb') as f:
                t = f.read()

            status = pickle.loads(t)
            self.spider_paused = status['spider_paused']
            self.spider_stopped = status['spider_stopped']
            self.spider_started = status['spider_started']
            self.all_spider_set = [self.spider_started, self.spider_stopped, self.spider_paused]
            self.logger.log_it("[load_status]{}".format(status))

    def on_begin(self):
        """Run before begin to do something like load tasks,or load config"""
        self.load_count()
        self.load_status()

    async def on_end(self):
        """Run when exit to do something like dump queue etc... It make self.done_all_things=True in last """
        ret = True
        if catty.config.PERSISTENCE['PERSIST_BEFORE_EXIT']:
            while await self.parser_scheduler_queue.qsize():
                ret &= dump_task(await get_task(self.parser_scheduler_queue), catty.config.DUMP_PATH, 'parser_scheduler',
                                 'Default')

        ret &= self.dump_count()
        ret &= self.dump_status()
        self.done_all_things = True

    async def _run_ins_func(self, spider_name: str, method_name: str, task: dict):
        """run the spider_ins boned method to parser it & push it to parser-request queue"""
        self.logger.log_it("[_run_ins_func]Parser the {}.{}".format(spider_name, method_name))
        _response = task['response']
        try:
            # get the instantiation spider from dict
            spider_ins = self.spider_module_handle.spider_instantiation[spider_name]
        except IndexError:
            self.logger.log_it("[_run_ins_func]No this Spider or had not instance yet.", 'WARN')
            return

        # get the spider's method from name
        method = spider_ins.__getattribute__(method_name)

        # get the method'parms
        _signature = inspect.signature(method).parameters

        if 'loop' in _signature:
            if 'task' in _signature:
                parser_return = await method(_response, task=task, loop=self.loop)
            else:
                parser_return = await method(_response, loop=self.loop)
        else:
            if 'task' in _signature:
                parser_return = method(_response, task=task)
            else:
                parser_return = method(_response)

        # if return a dict(normal)
        if isinstance(parser_return, dict):
            task['parser'].update({'item': parser_return})
            await push_task(self.parser_scheduler_queue, task, self.loop)
        elif parser_return is None:
            pass

    async def make_tasks(self):
        """run the done task & push them"""
        # FIXME as some as Scheduler
        task = await get_task(self.downloader_parser_queue)
        if task:
            self.loop.create_task(
                self.make_tasks()
            )
            if 200 <= task['response']['status'] < 400:
                self.counter.add_success(task['spider_name'])
                if task['spider_name'] in self.spider_started:
                    callback = task['callback']
                    spider_name = task['spider_name']
                    for callback_method_name in callback:
                        # number of task that parser return depend on the number of callbacks
                        each_task = deepcopy(task)
                        parser_method_name = callback_method_name.get('parser', None)

                        if parser_method_name:
                            try:
                                await self._run_ins_func(spider_name, parser_method_name, each_task)
                            except Retry_current_task:
                                # handle it like a new task
                                await push_task(self.parser_scheduler_queue, each_task, self.loop)
                            except:
                                # The except from user spiders
                                traceback.print_exc()
                elif task['spider_name'] in self.spider_paused:
                    # persist
                    dump_task(task, catty.config.DUMP_PATH, 'parser_scheduler', task['spider_name'])
                elif task['spider_name'] in self.spider_stopped:
                    pass
            else:
                # TODO retry it or do somethings else.
                self.counter.add_fail(task['spider_name'])

        else:
            await asyncio.sleep(LOAD_QUEUE_INTERVAL, loop=self.loop)

            self.loop.create_task(
                self.make_tasks()
            )

    def run_parser(self):
        self.loop.create_task(self.make_tasks())
        self.loop.create_task(self.counter.update())
        self.loop.run_forever()

    def run_persist(self):
        self.loop.create_task(self.load_persist_task())

    def run(self):
        try:
            self.on_begin()
            handler_server_thread = threading.Thread(target=self.run_handler)
            handler_server_thread.start()
            parser_thread = threading.Thread(target=self.run_parser)
            parser_thread.start()
            parser_thread.join()
        except KeyboardInterrupt:
            self.logger.log_it("[Ending]Doing the last thing...")
            self.loop.create_task(self.on_end())
            while not self.done_all_things:
                time.sleep(1)
            self.logger.log_it("Bye!")
            os._exit(0)


class BaseParser(object):
    @staticmethod
    def retry_current_task():
        raise Retry_current_task
