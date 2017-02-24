#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/24 13:14

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
        self._items = []
        self._task = []

        self.tasker = Tasker()

    def _load_spider(self):
        """load script from db"""
        pass

    def _load_item(self):
        """load item from parser-scheduler queue"""
        pass

    def _push_task(self):
        """push task to scheduler-downloader queue"""
        pass

    def _select_task(self):
        """base on the exetime and priority,select the task to psuh"""
        pass

    def unpack_items(self, item):
        """unpack callbacks ect. from items"""
        pass

    def _make_task(self, spider_ins, func, items):
        """run the spider_ins boned method to return a task,and append it to self._task"""

        self.tasker.make()

    def run(self):
        pass

    def loop(self):
        pass


if __name__ == '__main__':
    from catty.demo.spider import MySpider

    tasker = Tasker()
    spider = MySpider()
    a = spider.start()
    callbacks = tasker.make(a)['callbacks']
    for callback in callbacks:
        callback['fetcher']([('a','b')])