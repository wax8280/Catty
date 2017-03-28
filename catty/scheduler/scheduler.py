#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/24 13:14

import threading
import time
import traceback
from queue import PriorityQueue

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

        self._stop = False
        self.task_from_parser = PriorityQueue()
        # _task_to_downloader:the task waiting for exetime
        self.task_to_downloader = PriorityQueue()
        # _selected_task:the task already after exetime
        self.selected_task = PriorityQueue()

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

    # ---------------------Thread async queue-----------------
    async def load_task(self):
        """load item from parser-scheduler queue in a loop,append it to self._task_from_parser"""
        try:
            t = await self.parser_scheduler_queue.get()
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

        done = False
        while not done:
            try:
                self.task_from_parser.put_nowait(t)
                done = True
            except QueueFull:
                # print('[load_task]inner queue is full.')
                pass
            except Exception:
                traceback.print_exc()
                await asyncio.sleep(LOAD_QUEUE_SLEEP, loop=self.loop)

        print('[load_task]Load item')
        self.loop.create_task(
            self.load_task()
        )
        self.loop.create_task(
            self.push_task()
        )

    async def push_task(self):
        """ push task to scheduler-downloader queue in a loop"""
        try:
            t = self.selected_task.get_nowait()
        except QueueEmpty:
            # print('[push_task]inner queue is empty.')
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
                await self.scheduler_downloader_queue.put(t)
                done = True
            except AsyncQueueFull:
                traceback.print_exc()
                # print('[push_task]Redis queue is full')
                await asyncio.sleep(LOAD_QUEUE_SLEEP, loop=self.loop)
            except Exception as e:
                traceback.print_exc()
                await asyncio.sleep(LOAD_QUEUE_SLEEP, loop=self.loop)

        print('[push_task]Push item')
        self.loop.create_task(
            self.push_task()
        )
        self.loop.create_task(
            self.load_task()
        )

    # ---------------------------------------------------------

    # ---------------------Thread scheduler main-----------------
    def _make_task_from_request(self, request):
        """make the task from request and append it to self._task_to_downloader"""
        self.task_to_downloader.put(
            self.tasker.make(request)
        )

    def _run_ins_func(self, spider_name, method_name, item=None):
        """run the spider_ins boned method to return a task,and append it to self._task"""
        try:
            spider_ins = self.spider_module_handle.spider_instantiation[spider_name]
        except IndexError:
            print('No this Spider or had not instance')

        method = spider_ins.__getattribute__(method_name)
        if item:
            func_return_item = method(item=item)
        else:
            func_return_item = method()

        if isinstance(func_return_item, dict):
            self._make_task_from_request(func_return_item)

        # return how many request mean it make how many task
        elif isinstance(func_return_item, list):
            for each_func_return_item in func_return_item:
                self._make_task_from_request(each_func_return_item)

    def make_tasks(self):
        """get old task from self._task_from_parser and run ins func to  make new task to append it to self._task"""
        try:
            task = self.task_from_parser.get(timeout=3)
        except QueueEmpty:
            # print('[make_tasks][QueueEmpty]')
            return

        callback = task['callback']
        spider_name = task['spider_name']

        for callback_method_name in callback:
            fetcher_method_name = callback_method_name.get('fetcher', None)
            item = task['parser']['item']

            # TODO 判断result_pipiline_func是否是可迭代的（可以是list）（在parser里面判断）
            if fetcher_method_name:
                if not isinstance(fetcher_method_name, list):
                    fetcher_method_name = [fetcher_method_name]
                for each_fecher_method_name in fetcher_method_name:
                    try:
                        # 这里重新生成一个task，如果用户需要保存上一个task的数据（例如meta）必须自己手动处理
                        print('[make_tasks] {}.{} making task'.format(spider_name, each_fecher_method_name))
                        self._run_ins_func(
                            spider_name,
                            each_fecher_method_name,
                            item
                        )
                    except:
                        # TODO 这里捕获所有用户编写的spider里面的错误
                        traceback.print_exc()
                        # TODO 这里去重

    def make_task_from_start(self):
        """run spider_ins start method when a new spider start"""
        for spider_name, value in self.loaded_spider.items():
            if value['status'] == READY_TO_START:
                # start the spider's start method
                try:
                    print('[make_task_from_start]starting {}'.format(spider_name))
                    self._run_ins_func(
                        spider_name,
                        'start'
                    )
                    value['status'] = STARTED
                except:
                    # TODO 这里捕获所有用户编写的spider里面的错误
                    traceback.print_exc()

    # ----------------------------------------------------------

    # def _stop_spider(self, spider_name):
    #     """stop the spider and persist the task"""
    #     if spider_name in self.spider_started and spider_name not in self.spider_stoped:
    #         self.spider_stoped.add(spider_name)
    #         self.spider_started.remove(spider_name)
    #
    # def _start_spider(self, spider_name):
    #     """stop the spider and persist the task"""
    #     if spider_name not in self.spider_started:
    #         self.spider_stoped.add(spider_name)
    #         if spider_name in self.spider_stoped:
    #             self.spider_started.remove(spider_name)
    #
    # def handle(self):
    #     """accept some command to start or stop the spider"""
    #     pass

    def run_async_queue(self):
        self.loop.create_task(self.push_task())
        self.loop.create_task(self.load_task())
        # self.loop.run_forever()

    def run_task_maker(self):
        while True:
            self.make_task_from_start()
            self.make_tasks()

    def run_selector(self):
        while True:
            self.select_task()

    def run(self):
        t1 = threading.Thread(target=self.run_async_queue)
        t2 = threading.Thread(target=self.run_task_maker)
        t3 = threading.Thread(target=self.run_selector)

        t1.start()
        t2.start()
        t3.start()

        self.loop.run_forever()
