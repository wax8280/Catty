#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/26 10:05
import time
import traceback

from catty.exception import *
from catty.message_queue.redis_queue import RedisPriorityQueue


class Parser(object):
    LOAD_SPIDER_INTERVAL = 0.3

    def __init__(self,
                 downloader_parser_queue: RedisPriorityQueue,
                 parser_scheduler_queue: RedisPriorityQueue):
        self.downloader_parser_queue = downloader_parser_queue
        self.parser_scheduler_queue = parser_scheduler_queue

        self._count_pre_loop = 30

        self._task_from_downloader = []
        self._task_to_scheduler = []

    def _get_task_from_downloader_parser_queue(self):
        """load item from downloader_parser queue"""
        return self.downloader_parser_queue.get_nowait()

    def load_task(self):
        """load TASK from downloader_parser queue in a loop,append it to self._task_from_downloader"""
        try:
            self._task_from_downloader.append(
                    self._get_task_from_downloader_parser_queue()
            )
        except:
            traceback.print_exc()

    def _run_ins_func(self, func, task):
        """run the spider_ins boned method to parser it and return a item,and append it to self._task"""
        _response = task['response']['response_obj']
        parser_return = func(_response, task=task)

        # if return a dict(normal)
        if isinstance(parser_return, dict):
            task['parser'].update({'item': parser_return})
            self._task_to_scheduler.append(task)

    def make_tasks(self):
        """get old task from self._task_from_downloader and make new task to append it to self._task_to_scheduler"""
        try:
            task = self._task_from_downloader.pop()
        except IndexError:
            return

        callback = task['callbacks']
        parser_func = callback.get('parser', None)
        # TODO 判断result_pipiline_func是否是可迭代的（可以是list）
        try:
            self._run_ins_func(parser_func, task)
        except Retry_current_task:
            # handle it like a new task
            self._task_to_scheduler.append(task)
        except:
            traceback.print_exc()
            pass

    def _loop(self):
        __c__ = 0
        while not self.downloader_parser_queue and __c__ < self._count_pre_loop:
            __c__ += 1
            self.load_task()
            self.make_tasks()

    def run(self):
        while True:
            self._loop()
            time.sleep(self.LOAD_SPIDER_INTERVAL)
