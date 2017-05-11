#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/05/06 12:31
import pickle
from catty.libs.utils import md5string, Task


class Tasker(object):
    @staticmethod
    def make_task(request: dict):
        spider_name = request['spider_name']
        priority = request['priority']
        callback = request['callback']
        meta = request['meta']

        tid = md5string(request['request']['url'] + str(request['request']['data']) + str(request['request']['params']))

        return Task(
            tid=tid,
            spider_name=spider_name,
            priority=priority,
            meta=meta,
            request=request['request'],
            downloader={},
            scheduler={},
            parser={},
            response={},
            callback=callback,
        )

    @staticmethod
    def dump_task(task):
        return pickle.dumps(task)

    @staticmethod
    def load_task(dumped_task):
        return pickle.loads(dumped_task)
