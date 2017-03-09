#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/3/9 17:06

import asynctest
from catty.message_queue.redis_queue import AsyncRedisPriorityQueue
from catty.downloader.downloader import Crawler

class Test(asynctest.TestCase):
    use_default_loop = True

    async def setUp(self):
        pass

    async def tearDown(self):
        pass
