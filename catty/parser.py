#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/26 10:05
import asyncio
import inspect
import os
import threading
import time
import traceback
from asyncio import BaseEventLoop
from copy import deepcopy
from functools import partial

import catty.config
from catty import PARSER_SCHEDULER, DOWNLOADER_PARSER
from catty.message_queue import AsyncRedisPriorityQueue, get_task, push_task
from catty.handler import HandlerMixin
from catty.exception import Retry_current_task
from catty.libs.count import Counter
from catty.libs.handle_module import SpiderModuleHandle
from catty.libs.log import Log
from catty.libs.utils import dump_task, load_task, dump_pickle_data, load_pickle_data


class Parser(HandlerMixin):
    def __init__(self,
                 downloader_parser_queue: AsyncRedisPriorityQueue,
                 parser_scheduler_queue: AsyncRedisPriorityQueue,
                 scheduler_downloader_queue: AsyncRedisPriorityQueue,
                 loop: BaseEventLoop,
                 name: str):
        """
        :param downloader_parser_queue:The redis queue
        :param parser_scheduler_queue:The redis queue
        :param loop:EventLoop
        """
        super(Parser, self).__init__()
        self.name = name

        self.downloader_parser_queue = downloader_parser_queue
        self.parser_scheduler_queue = parser_scheduler_queue
        self.scheduler_downloader_queue = scheduler_downloader_queue
        self.loop = loop

        self.spider_started = set()
        self.spider_stopped = set()
        self.spider_paused = set()

        self.all_spider_set = [self.spider_started, self.spider_stopped, self.spider_paused]

        self.spider_module_handle = SpiderModuleHandle(catty.config.SPIDER_PATH)
        self.spider_module_handle.load_all_spider()

        self.logger = Log('Parser')
        self.ready_to_exit = False
        self.counter = Counter(loop)

    async def load_tasks(self, spider_name: str, which_q: str = ''):
        """load the persist task & push it to queue"""
        tasks = await load_task(catty.config.PERSISTENCE['DUMP_PATH'], '{}_{}'.format(self.name, which_q), spider_name)
        if tasks:
            self.logger.log_it("[load_tasks]Load tasks:{}".format(tasks))
            for each_task in tasks:
                # push each task to request-queue
                if which_q == PARSER_SCHEDULER:
                    await push_task(self.parser_scheduler_queue, each_task, self.loop)
                elif which_q == DOWNLOADER_PARSER:
                    await push_task(self.downloader_parser_queue, each_task, self.loop)

    async def dump_tasks(self, which_q: str):
        """ dump the task which in queue """
        if which_q == PARSER_SCHEDULER:
            while await self.parser_scheduler_queue.qsize():
                task = await get_task(self.parser_scheduler_queue)
                if task is not None:
                    await dump_task(task, catty.config.PERSISTENCE['DUMP_PATH'], "{}_{}".format(self.name, which_q),
                                    task['spider_name'])
                    self.logger.log_it("[dump_task]Dump task:{}".format(task))
        elif which_q == DOWNLOADER_PARSER:
            while await self.downloader_parser_queue.qsize():
                task = await get_task(self.downloader_parser_queue)
                if task is not None:
                    await dump_task(task, catty.config.PERSISTENCE['DUMP_PATH'], "{}_{}".format(self.name, which_q),
                                    task['spider_name'])
                    self.logger.log_it("[dump_task]Dump task:{}".format(task))

    def dump_count(self):
        counter_date = {'value_d': self.counter.value_d, 'cache_value': self.counter.cache_value}
        root = os.path.join(catty.config.PERSISTENCE['DUMP_PATH'], 'parser')
        dump_pickle_data(root, '{}_counter'.format(self.name), counter_date)
        self.logger.log_it("[dump_count]{}".format(counter_date))

    def load_count(self):
        root = os.path.join(catty.config.PERSISTENCE['DUMP_PATH'], 'parser')
        counter_date = load_pickle_data(root, '{}_counter'.format(self.name))
        self.counter.value_d = counter_date['value_d']
        self.counter.cache_value = counter_date['cache_value']
        self.logger.log_it("[load_count]{}".format(counter_date))

    def dump_status(self):
        status = {
            'spider_started': self.spider_started,
            'spider_paused': self.spider_paused,
            'spider_stopped': self.spider_stopped
        }
        root = os.path.join(catty.config.PERSISTENCE['DUMP_PATH'], 'parser')
        dump_pickle_data(root, '{}_status'.format(self.name), status)
        self.logger.log_it("[dump_status]{}".format(status))

    def load_status(self):
        root = os.path.join(catty.config.PERSISTENCE['DUMP_PATH'], 'parser')

        status = load_pickle_data(root, '{}_status'.format(self.name))
        self.spider_paused = status['spider_paused']
        self.spider_stopped = status['spider_stopped']
        self.spider_started = status['spider_started']
        self.all_spider_set = [self.spider_started, self.spider_stopped, self.spider_paused]
        self.logger.log_it("[load_status]{}".format(status))

    def on_begin(self):
        """Run before begin to do something like load tasks,or load config"""
        self.load_status()
        self.load_count()
        for started_spider in self.spider_started:
            self.loop.create_task(self.load_tasks(DOWNLOADER_PARSER, started_spider))
            self.loop.create_task(self.load_tasks(PARSER_SCHEDULER, started_spider))

    async def check_end(self):
        done = []
        while not done or False in done:
            done = []
            psq = await self.parser_scheduler_queue.qsize()
            dpq = await self.downloader_parser_queue.qsize()
            if psq == 0 or psq is None:
                await asyncio.sleep(0.5, self.loop)
                done.append(True)
            else:
                done.append(False)

            if dpq == 0 or dpq is None:
                await asyncio.sleep(0.5, self.loop)
                done.append(True)
            else:
                done.append(False)

        self.ready_to_exit = True

    async def on_end(self):
        """Run when exit to do something like dump queue etc... It make self.done_all_things=True in last """
        # no need to
        # for task in asyncio.Task.all_tasks():
        #     task.cancel()

        if catty.config.PERSISTENCE['PERSIST_BEFORE_EXIT']:
            self.loop.create_task(self.dump_tasks(PARSER_SCHEDULER))
            self.loop.create_task(self.dump_tasks(DOWNLOADER_PARSER))

        self.dump_count()
        self.dump_status()

        self.loop.create_task(self.check_end())

    def get_spider_method(self, spider_name: str, method_name: str):
        """Return a bound method if spider have this method,return None if not."""
        try:
            # get the instantiation spider from dict
            spider_ins = self.spider_module_handle.spider_instantiation[spider_name]
        except IndexError:
            self.logger.log_it("[_run_ins_func]No this Spider or had not instance yet.", 'WARN')
            return

        # get the spider's method from name
        method = spider_ins.__getattribute__(method_name)

        return method, spider_ins

    async def _run_ins_func(self, spider_name: str, method_name: str, task: dict):
        """run the spider_ins boned method to parser it & push it to parser-scheduler queue"""
        self.logger.log_it("[_run_ins_func]Parser the {}.{}".format(spider_name, method_name))
        _response = task['response']

        method, _ = self.get_spider_method(spider_name, method_name)
        # get the method'parms
        _signature = inspect.signature(method).parameters

        try:
            # the async method define by user must have a loop param even never use it.
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
        except Retry_current_task:
            # handle it like a new task
            self.loop.create_task(push_task(self.parser_scheduler_queue, task, self.loop))
            return
        except:
            # The except from user spiders
            traceback.print_exc()
            return

        if isinstance(parser_return, dict):
            # normal
            if 'tid' not in parser_return:
                # not a task
                task['parser'].update({'item': parser_return})
            await push_task(self.parser_scheduler_queue, task, self.loop)
        elif isinstance(parser_return, list):
            # task_list
            for each_return_task in parser_return:
                await push_task(self.scheduler_downloader_queue, each_return_task, self.loop)
        elif parser_return is None:
            pass

    async def make_tasks(self):
        """run the done task & push them"""
        task = await get_task(self.downloader_parser_queue)
        if task:
            self.loop.create_task(self.make_tasks())
            if 'status' in task['response']:
                if 200 <= task['response']['status'] < 400 and \
                                task['response']['status'] in task['handle_status_code']:
                    self.counter.add_success(task['spider_name'])
                    if task['spider_name'] in self.spider_started:
                        callback = task['callback']
                        spider_name = task['spider_name']
                        for callback_method_name in callback:
                            # number of task that parser return depend on the number of callbacks
                            each_task = deepcopy(task)
                            parser_method_name = callback_method_name.get('parser', None)

                            if parser_method_name:
                                self.loop.create_task(self._run_ins_func(spider_name, parser_method_name, each_task))

                    elif task['spider_name'] in self.spider_paused:
                        # persist
                        await dump_task(task, catty.config.PERSISTENCE['DUMP_PATH'], 'parser', task['spider_name'])
                    elif task['spider_name'] in self.spider_stopped:
                        pass
                else:
                    retry = task['meta']['retry']
                    retried = task.get('retried', 0)
                    if retry != 0 and retried < retry:
                        task.update({'retried': retried + 1})
                        retry_method, _ = self.get_spider_method(task['spider_name'], 'retry')

                        # it could be return a list
                        retry_tasks = retry_method(task)
                        if not isinstance(retry_tasks, list):
                            retry_tasks = [retry_tasks]
                        for each_retry_task in retry_tasks:
                            await asyncio.sleep(task['meta']['retry_wait'], self.loop)
                            await push_task(self.scheduler_downloader_queue, each_retry_task, self.loop)
                    self.counter.add_fail(task['spider_name'])

        else:
            # if no task in downloader_parser queue,wait it
            self.loop.call_later(catty.config.LOAD_QUEUE_INTERVAL, lambda: self.loop.create_task(self.make_tasks()))

    def quit(self):
        self.logger.log_it("[Ending]Doing the last thing...", level='INFO')
        self.loop.create_task(self.on_end())
        while not self.ready_to_exit:
            time.sleep(1)
        self.logger.log_it("Bye!", level='INFO')
        os._exit(0)

    def run_parser(self):
        for i in range(catty.config.NUM_OF_PARSER_MAKE_TASK):
            self.loop.create_task(self.make_tasks())
        self.loop.create_task(self.counter.update())
        self.loop.run_forever()

    def run(self):
        try:
            self.on_begin()
            xmlrpc_partial_func = partial(self.xmlrpc_run, name=self.name)
            handler_server_thread = threading.Thread(target=xmlrpc_partial_func)
            parser_thread = threading.Thread(target=self.run_parser)
            parser_thread.start()
            handler_server_thread.start()

            # In Windows,I cant catch KeyboardInterrupt.FXXK!
            while True:
                r = input()
                if r == 'Q':
                    self.quit()
        except KeyboardInterrupt:
            self.quit()


class BaseParser(object):
    @staticmethod
    def retry_current_task():
        raise Retry_current_task

    def retry(self, task):
        return task
