#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/26 10:05
import inspect
import threading
import traceback
import time
import os
from copy import deepcopy
from asyncio import BaseEventLoop

from catty.config import CONFIG
from catty.exception import *
from catty.handler import HandlerMixin
from catty.libs.handle_module import SpiderModuleHandle
from catty.libs.log import Log
from catty.libs.utils import dump_task, load_task
from catty.libs.count import Counter
from catty.message_queue.redis_queue import AsyncRedisPriorityQueue, get_task, push_task
from catty import LOAD_QUEUE_SLEEP, PARSER


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
        self.name = PARSER

        self.downloader_parser_queue = downloader_parser_queue
        self.parser_scheduler_queue = parser_scheduler_queue
        self.loop = loop

        self.spider_started = set()
        self.spider_stopped = set()
        self.spider_paused = set()

        self.spider_module_handle = SpiderModuleHandle(CONFIG['SPIDER_PATH'])
        self.spider_module_handle.load_all_spider()

        self.logger = Log(PARSER)
        self.done_all_things = False
        self.counter = Counter(loop)

    async def conn_redis(self):
        """Conn the redis"""
        await self.downloader_parser_queue.conn()
        await self.parser_scheduler_queue.conn()

    async def load_task(self):
        """load the persist task & push it to request-queue"""
        # TODO dump downloader-parser queue
        # load the parser_scheduler dumped files.
        tasks = [load_task(CONFIG['DUMP_PATH'], 'parser_scheduler', spider_name) for spider_name in self.spider_started]
        self.logger.log_it("[load_task]Load tasks:{}".format(tasks))

        if tasks and tasks[0] is not None:
            for spider_tasks in tasks:
                for each_task in spider_tasks:
                    # push each task to parser-scheduler queue
                    await push_task(self.parser_scheduler_queue, each_task, self.loop)

    async def ending_handle(self):
        """Run when exit to do something like dump queue etc... It make self.done_all_things=True in last """
        # FIXME this method only called in exit
        while await self.parser_scheduler_queue.qsize():
            dump_task(await get_task(self.parser_scheduler_queue), CONFIG['DUMP_PATH'], 'parser_scheduler', 'Default')

        # TODO persist count
        self.done_all_things = True

    async def _run_ins_func(self, spider_name, method_name, task):
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

        if 'response' in _signature:
            if 'task' in _signature:
                # TODO task to do somethings such as retrying etc... in the next version
                parser_return = method(_response, task=task)
            else:
                parser_return = method(_response)

            # if return a dict(normal)
            if isinstance(parser_return, dict):
                task['parser'].update({'item': parser_return})
                await push_task(self.parser_scheduler_queue, task, self.loop)
        else:
            self.logger.log_it(message="[_run_ins_func]A parser method must have a 'response' param.", level='WARN')

    async def make_tasks(self):
        """run the done task & push them"""
        # FIXME as some as Scheduler
        task = await get_task(self.downloader_parser_queue)
        if task:
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
                    dump_task(task, CONFIG['DUMP_PATH'], 'parser_scheduler', task['spider_name'])
                elif task['spider_name'] in self.spider_stopped:
                    pass
            else:
                # TODO retry it or do somethings else.
                self.counter.add_fail(task['spider_name'])
        else:
            await asyncio.sleep(LOAD_QUEUE_SLEEP, loop=self.loop)

        self.loop.create_task(
            self.make_tasks()
        )

    def run_parser(self):
        self.loop.create_task(self.make_tasks())
        self.loop.create_task(self.counter.update())
        self.loop.run_forever()

    def run_persist(self):
        self.loop.create_task(self.load_task())

    def run(self):
        try:
            handler_server_thread = threading.Thread(target=self.run_handler)
            handler_server_thread.start()
            persist_thread = threading.Thread(target=self.run_persist)
            persist_thread.start()
            parser_thread = threading.Thread(target=self.run_parser)
            parser_thread.start()
            parser_thread.join()
        except KeyboardInterrupt:
            self.logger.log_it("[Ending]Doing the last thing...")
            self.loop.create_task(self.ending_handle())
            while not self.done_all_things:
                time.sleep(1)
            self.logger.log_it("Bye!")
            os._exit(0)
