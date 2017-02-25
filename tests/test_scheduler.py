#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/25 10:30

import unittest

from catty.message_queue.redis_queue import RedisPriorityQueue
from catty.scheduler.scheduler import Scheduler


class Test(unittest.TestCase):
    def setUp(self):
        self.scheduler_downloader_queue = RedisPriorityQueue('MySpider:SD')
        self.parser_scheduler_queue = RedisPriorityQueue('MySpider:PS')
        self.scheduler = Scheduler(
                self.scheduler_downloader_queue,
                self.parser_scheduler_queue,
        )

    def test_(self):
        pass



if __name__ == '__main__':
    unittest.main()
