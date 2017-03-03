#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/24 13:14

import time
import traceback

from catty.message_queue.redis_queue import RedisPriorityQueue
from catty.scheduler.tasker import Tasker

NOTSTART = 0
STARTED = 1
READY_TO_START = 2
STOP = 3


class Scheduler(object):
    LOAD_SPIDER_INTERVAL = 1
    # schema
    # [
    #   {
    #       'spider': spdier_ins,
    #       'name': spider_name
    #       'status': int,   # 0 not_start 1 start
    #   }
    # ]
    SPIDERS = []
    COUNT_PRE_LOOP = 30

    def __init__(self,
                 scheduler_downloader_queue: RedisPriorityQueue,
                 parser_scheduler_queue: RedisPriorityQueue):
        self.scheduler_downloader_queue = scheduler_downloader_queue
        self.parser_scheduler_queue = parser_scheduler_queue

        self._stop = False
        self._count_pre_loop = self.COUNT_PRE_LOOP
        self._task_from_parser = []
        # _task_to_downloader:the task waiting for exetime
        self._task_to_downloader = []
        # _selected_task:the task already after exetime
        self._selected_task = []

        self.tasker = Tasker()
        # [
        #   {'spider_name':string,'spider_class':class}
        # ]
        self.spdier_loaded = []

        self.spider_insed = set()
        self.spider_stoped = set()
        self.spider_started = set()

    def load_spider(self):
        """load script from db"""
        pass

    def _instantiate_spdier(self, spider_class, spider_name, status=0):
        """instantiate the spider"""
        self.SPIDERS.append({
            'spider': spider_class(),
            'name': spider_name,
            'status': status
        })

    def instantiate_spdier(self):
        """instantiate all spider which had loaded and not instantiated"""
        for spider in self.spdier_loaded:
            if spider['spider_name'] not in self.spider_insed:
                self._instantiate_spdier(
                        spider['spider_class'],
                        spider['spider_name'],
                )

    def _select_task(self):
        """base on the exetime select the task to push"""
        for each_task in self._task_to_downloader:
            if time.time() - each_task['scheduler']['exetime'] > 0:
                self._selected_task.append(each_task)

    def _get_item_from_parser_scheduler_queue(self):
        """get item from parser-scheduler queue"""
        return self.parser_scheduler_queue.get_nowait()

    def _push_task_to_scheduler_parser_queue(self):
        """push task to scheduler-downloader queue"""
        try:
            t = self._selected_task.pop()
        except IndexError:
            return

        try:
            self.scheduler_downloader_queue.put_nowait(t)
        except:
            traceback.print_exc()
            self._selected_task.append(t)
            # TODO log it

    def load_task_from_queue(self):
        """load item from parser-scheduler queue in a loop,append it to self._task_from_parser"""
        try:
            self._task_from_parser.append(
                    self._get_item_from_parser_scheduler_queue()
            )
        except:
            traceback.print_exc()

    def select_and_push_task(self):
        """select the task which in up to exetime push task to scheduler-downloader queue in a loop"""
        self._select_task()
        self._push_task_to_scheduler_parser_queue()

    def _make_task_from_request(self, request):
        """make the task from request and append it to self._task_to_downloader"""
        self._task_to_downloader.append(
                self.tasker.make(request)
        )

    def _run_ins_func(self, func, items=None):
        """run the spider_ins boned method to return a task,and append it to self._task"""
        if items:
            request = func(items=items)
        else:
            request = func()

        if isinstance(request, dict):
            self._make_task_from_request(request)
        elif isinstance(request, list):
            for each_request in request:
                self._make_task_from_request(each_request)

    def make_tasks(self):
        """get old task from self._task_from_parser and run ins func to  make new task to append it to self._task"""
        try:
            task = self._task_from_parser.pop()
        except IndexError:
            return
        callback = task['callbacks']
        fetcher_func = callback.get('fetcher', None)
        # TODO 判断result_pipiline_func是否是可迭代的（可以是list）
        try:
            self._run_ins_func(fetcher_func, task['parser']['item'])
        except:
            # TODO 这里捕获所有用户编写的spider里面的错误
            traceback.print_exc()
            pass

    def make_task_from_start(self):
        """run spdier_ins start method when a new spider start"""
        for spider_ins in self.SPIDERS:
            if spider_ins['status'] == READY_TO_START:
                # start the spider's start method
                self._run_ins_func(spider_ins['spider'].start)
                spider_ins['status'] = STARTED

    def _stop_spider(self, spider_name):
        """stop the spider and persist the task"""
        if spider_name in self.spider_started and spider_name not in self.spider_stoped:
            self.spider_stoped.add(spider_name)
            self.spider_started.remove(spider_name)

    def _start_spider(self,spider_name):
        """stop the spider and persist the task"""
        if spider_name not in self.spider_started:
            self.spider_stoped.add(spider_name)
            if spider_name in self.spider_stoped:
                self.spider_started.remove(spider_name)

    def handle(self):
        """accept some command to start or stop the spider"""
        pass

    def run(self):
        pass

    def _loop(self):
        __c__ = 0
        while (not self.parser_scheduler_queue.empty()
               and not self.scheduler_downloader_queue.full()
               and __c__ < self._count_pre_loop
               ):
            try:
                # 接收控制信息->读取spider->实例化spider->运行新的spider的start方法->
                # 从队列里读取parser发过来的task->运行函数并经过request生成新的task->等待执行时间到，将task放入队列
                self.handle()
                self.load_spider()
                self.instantiate_spdier()
                self.make_task_from_start()
                self.load_task_from_queue()
                self.make_tasks()
                self.select_and_push_task()
            except KeyboardInterrupt:
                pass


if __name__ == '__main__':
    tasker = Tasker()
    # spider = MySpider()
    # dumped = tasker.dump_task(tasker.make(spider.start())['callbacks'])
    # print(dumped)

    # a=tasker.load_task(b'\x80\x03]q\x00(}q\x01(X\x06\x00\x00\x00parserq\x02cbuiltins\ngetattr\nq\x03ccatty.demo.spider\nMySpider\nq\x04)\x81q\x05X\x13\x00\x00\x00parser_content_pageq\x06\x86q\x07Rq\x08X\x07\x00\x00\x00fetcherq\th\x03h\x05X\x0b\x00\x00\x00get_contentq\n\x86q\x0bRq\x0cu}q\r(h\x02h\x03h\x05X\x10\x00\x00\x00parser_list_pageq\x0e\x86q\x0fRq\x10h\th\x03h\x05X\x08\x00\x00\x00get_listq\x11\x86q\x12Rq\x13X\x0f\x00\x00\x00result_pipelineq\x14h\x03h\x05X\t\x00\x00\x00save_listq\x15\x86q\x16Rq\x17ue.')
    # print(a[0]['parser']('response'))
