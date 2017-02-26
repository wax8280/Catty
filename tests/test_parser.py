#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/26 19:57
#!/usr/bin/env python

import unittest

from catty.message_queue.redis_queue import RedisPriorityQueue
from catty.parser.parser import Parser


class Test(unittest.TestCase):
    def setUp(self):
        self.downloader_parser_queue = RedisPriorityQueue('MySpider:DP')
        self.parser_scheduler_queue = RedisPriorityQueue('MySpider:PS')

        self.parser = Parser(
                self.downloader_parser_queue,
                self.parser_scheduler_queue,
        )

    def tearDown(self):
        self.downloader_parser_queue.clear()
        self.parser_scheduler_queue.clear()


if __name__ == '__main__':
    unittest.main()
