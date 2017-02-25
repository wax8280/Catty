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


class Scheduler(object):
    LOAD_SPIDER_INTERVAL = 1
    # [{'spdier_name':spdier_ins}]
    SPIDER_INS = []

    def __init__(self,
                 scheduler_downloader_queue: RedisPriorityQueue,
                 parser_scheduler_queue: RedisPriorityQueue):
        self.scheduler_downloader_queue = scheduler_downloader_queue
        self.parser_scheduler_queue = parser_scheduler_queue

        self._stop = False
        self._task_from_parser = []
        self._count_pre_loop = 30
        self._task_to_downloader = []
        self._selected_task = []

        self.tasker = Tasker()

    def _load_spider(self):
        """load script from db"""
        pass

    def _load_item(self):
        """load item from parser-scheduler queue"""
        return self.parser_scheduler_queue.get_nowait()

    def load_item(self):
        _c_ = 0
        while not self.parser_scheduler_queue.empty() and _c_ < self._count_pre_loop:
            _c_ += 1
            try:
                self._task_from_parser.append(
                        self._load_item()
                )
            except:
                traceback.print_exc()

    def _push_task(self):
        """push task to scheduler-downloader queue"""
        _c_ = 0
        while self._selected_task and _c_ < self._count_pre_loop:
            _c_ += 1
            try:
                t = self._selected_task.pop()
                self.scheduler_downloader_queue.put_nowait(t)
            except:
                traceback.print_exc()
                self._selected_task.append(t)
                # TODO log it

    def _select_task(self):
        """base on the exetime select the task to push"""
        for each_task in self._task_to_downloader:
            if time.time() - each_task['exetime'] > 0:
                self._selected_task.append(each_task)

    def unpack_items(self, item):
        """unpack items"""
        pass

    def _make_task(self, func, items):
        """run the spider_ins boned method to return a task,and append it to self._task"""
        task = func(items=items)
        self._task_to_downloader.append(task)

    def make_tasks(self):
        """get old task from self._task_from_parser and make new task to append it to self._task"""
        _c_ = 0
        while self._task_from_parser and _c_ < self._count_pre_loop:
            _c_ += 1
            task = self._task_from_parser.pop()
            for callback in task['callbacks']:
                parser_func = callback.get('parser', None)
                fetcher_func = callback.get('fetcher', None)
                result_pipeline_func = callback.get('result_pipeline', None)
                # TODO 判断result_pipiline_func是否是可迭代的（可以是list）
                try:
                    self._make_task(fetcher_func, task['items'])
                except:
                    traceback.print_exc()
                    pass

    def make_task_from_start(self):
        """run spdier_ins start method when a new spdier start"""
        pass

    def run(self):
        pass

    def loop(self):
        pass


if __name__ == '__main__':
    tasker = Tasker()
    # spider = MySpider()
    # dumped = tasker.dump_task(tasker.make(spider.start())['callbacks'])
    # print(dumped)

    # a=tasker.load_task(b'\x80\x03]q\x00(}q\x01(X\x06\x00\x00\x00parserq\x02cbuiltins\ngetattr\nq\x03ccatty.demo.spider\nMySpider\nq\x04)\x81q\x05X\x13\x00\x00\x00parser_content_pageq\x06\x86q\x07Rq\x08X\x07\x00\x00\x00fetcherq\th\x03h\x05X\x0b\x00\x00\x00get_contentq\n\x86q\x0bRq\x0cu}q\r(h\x02h\x03h\x05X\x10\x00\x00\x00parser_list_pageq\x0e\x86q\x0fRq\x10h\th\x03h\x05X\x08\x00\x00\x00get_listq\x11\x86q\x12Rq\x13X\x0f\x00\x00\x00result_pipelineq\x14h\x03h\x05X\t\x00\x00\x00save_listq\x15\x86q\x16Rq\x17ue.')
    # print(a[0]['parser']('response'))
