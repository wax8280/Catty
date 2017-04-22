#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/4/10 19:05
import asyncio
from collections import deque


class Counter:
    def __init__(self, loop, value_d=None, cache_value=None):
        self.loop = loop
        self.interval = 60
        self.value_d = {} if not value_d else value_d
        self.cache_value = {} if not cache_value else cache_value
        self.max_size = 1440

    def add_success(self, name):
        self.value_d.setdefault(name + '_fail', 0)
        value = self.value_d.setdefault(name + '_success', 0)
        self.value_d.update({name + '_success': value + 1})

    def add_fail(self, name):
        self.value_d.setdefault(name + '_success', 0)
        value = self.value_d.setdefault(name + '_fail', 0)
        self.value_d.update({name + '_fail': value + 1})

    async def update(self):
        for name, count in self.value_d.items():
            q = self.cache_value.setdefault(name, deque(maxlen=self.max_size))
            q.append(count)
        self.value_d.clear()
        await asyncio.sleep(self.interval, loop=self.loop)
        self.loop.create_task(self.update())

    def count_all(self):

        result = {}
        for name, q in self.cache_value.items():
            if '_success' in name:
                result.update(
                    {name: (int(sum(q) / len(q)), int(sum(q) / len(q) * 5), int(sum(q) / len(q) * 60), int(sum(q)))})
            elif '_fail' in name:
                result.update(
                    {name: (int(sum(q) / len(q)), int(sum(q) / len(q) * 5), int(sum(q) / len(q) * 60), int(sum(q)))})
        return result
