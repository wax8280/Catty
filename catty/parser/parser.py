#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/26 10:05
import traceback

from catty.message_queue.redis_queue import RedisPriorityQueue


class Parser(object):
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
        _c_ = 0
        while not self.parser_scheduler_queue.empty() and _c_ < self._count_pre_loop:
            _c_ += 1
            try:
                self._task_from_downloader.append(
                        self._get_task_from_downloader_parser_queue()
                )
            except:
                traceback.print_exc()

    def _run_ins_func(self, func, task):
        """run the spider_ins boned method to parser it and return a item,and append it to self._task"""
        _response=task['response']['response_obj']
        item = func(_response, task=task)


        if isinstance(task, dict):
            self._task_to_downloader.append(task)
        elif isinstance(task, list):
            for each_task in task:
                self._task_to_downloader.append(each_task)

    def make_tasks(self):
        """get old task from self._task_from_parser and make new task to append it to self._task"""
        _c_ = 0
        while self._task_from_parser and _c_ < self._count_pre_loop:
            _c_ += 1
            task = self._task_from_parser.pop()
            callback = task['callbacks']
            parser_func = callback.get('parser', None)
            fetcher_func = callback.get('fetcher', None)
            result_pipeline_func = callback.get('result_pipeline', None)
            # TODO 判断result_pipiline_func是否是可迭代的（可以是list）
            try:
                self._run_ins_func(fetcher_func, task['item'])
            except:
                traceback.print_exc()
                pass