#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/26 10:05
import inspect
import traceback
from copy import deepcopy
from queue import PriorityQueue

from catty.config import CONFIG
from catty.exception import *
from catty.libs.handle_module import SpiderModuleHandle
from catty.libs.log import Log
from catty.message_queue.redis_queue import AsyncRedisPriorityQueue

LOAD_QUEUE_SLEEP = 0.5


class Parser(object):
    def __init__(self,
                 downloader_parser_queue: AsyncRedisPriorityQueue,
                 parser_scheduler_queue: AsyncRedisPriorityQueue,
                 loop):

        self.downloader_parser_queue = downloader_parser_queue
        self.parser_scheduler_queue = parser_scheduler_queue

        self.inner_in_q = PriorityQueue()
        self.inner_ou_q = PriorityQueue()

        self.loop = loop

        self.spider_module_handle = SpiderModuleHandle(CONFIG['SPIDER_PATH'])
        self.spider_module_handle.to_instance_spider()

        self.logger = Log('Parser')

    async def conn_redis(self):
        """Conn the redis"""
        await self.downloader_parser_queue.conn()
        await self.parser_scheduler_queue.conn()

    async def load_task(self):
        """load TASK from downloader-parser queue,put it in Parser own queue"""
        self.logger.log_it(message="[load_task]", level='INFO')

        try:
            t = await self.downloader_parser_queue.get()
        except AsyncQueueEmpty:
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

        done = False
        while not done:
            try:
                self.inner_in_q.put_nowait(t)
                done = True
            except QueueFull:
                # print('[load_task]inner queue is full.')
                await asyncio.sleep(LOAD_QUEUE_SLEEP, loop=self.loop)
            except Exception:
                traceback.print_exc()
                await asyncio.sleep(LOAD_QUEUE_SLEEP, loop=self.loop)

        self.logger.log_it(message="[load_task]Task:{}".format(t), level='DEBUG')
        self.loop.create_task(
            self.load_task()
        )
        # self.loop.create_task(
        #     self.push_task()
        # )

    async def push_task(self):
        """put TASK from Parser own queue to parser-scheduler queue"""
        self.logger.log_it(message="[push_task]", level='INFO')

        try:
            t = self.inner_ou_q.get_nowait()
        except QueueEmpty:
            await asyncio.sleep(LOAD_QUEUE_SLEEP, loop=self.loop)
            self.loop.create_task(
                self.push_task()
            )
            return
        except Exception:
            traceback.print_exc()
            await asyncio.sleep(LOAD_QUEUE_SLEEP, loop=self.loop)
            self.loop.create_task(
                self.push_task()
            )
            return

        done = False
        while not done:
            try:
                await self.parser_scheduler_queue.put(t)
                done = True
            except AsyncQueueFull:
                traceback.print_exc()
                await asyncio.sleep(LOAD_QUEUE_SLEEP, loop=self.loop)
            except Exception as e:
                traceback.print_exc()
                await asyncio.sleep(LOAD_QUEUE_SLEEP, loop=self.loop)

        self.logger.log_it(message="[push_task]Task:{}".format(t), level='DEBUG')
        self.loop.create_task(
            self.push_task()
        )
        # self.loop.create_task(
        #     self.load_task()
        # )

    def _run_ins_func(self, spider_name, method_name, task):
        """run the spider_ins boned method to parser it and return a item,and append it to self._task"""
        self.logger.log_it(message="[_run_ins_func]", level='INFO')
        _response = task['response']
        try:
            spider_ins = self.spider_module_handle.spider_instantiation[spider_name]
        except IndexError:
            self.logger.log_it(message="[_run_ins_func]No this Spider or had not instance", level='WARN')

        # get the spider's method from name
        method = spider_ins.__getattribute__(method_name)

        # get the method'parms
        _signature = inspect.signature(method).parameters
        assert 'response' in _signature

        # TODO task to do somethings such as retrying etc... in the next version
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
        self.logger.log_it(message="[make_tasks]", level='INFO')

        # (priority, task)
        task = self.inner_in_q.get()[1]

        callback = task['callback']
        spider_name = task['spider_name']
        for callback_method_name in callback:
            # number of task that parser return depend on the number of callbacks
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

    def run_async_queue(self):
        self.loop.create_task(self.push_task())
        self.loop.create_task(self.load_task())

    def run_parser(self):
        import time
        while True:
            now = time.time()
            self.make_tasks()
            print(time.time() - now)

    def run(self):
        import threading
        t1 = threading.Thread(target=self.run_async_queue)
        t2 = threading.Thread(target=self.run_parser)

        t1.start()
        t2.start()

        self.loop.run_forever()
