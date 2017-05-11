#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/24 14:26
import abc

import catty.config
from catty.libs.request import Request
from catty.libs.tasker import Tasker


class BaseSpider(metaclass=abc.ABCMeta):
    speed = catty.config.SPIDER_DEFAULT['SPEED']
    seeds = catty.config.SPIDER_DEFAULT['SEEDS']
    blocknum = catty.config.SPIDER_DEFAULT['BLOCKNUM']

    @abc.abstractmethod
    def start(self):
        pass

    def request(self, **kwargs):
        request = Request(**kwargs)
        callback = kwargs['callback']

        # method'name
        n_callback = []
        if isinstance(callback, dict):
            callback = [callback]

        for each_callback in callback:
            for k, v in each_callback.items():
                if not isinstance(v, list):
                    n_callback.append({k: v.__name__})
                else:
                    n_callback.append({k: [i.__name__ for i in v]})

        meta = kwargs.get('meta', {})

        meta.setdefault('retry', 0)
        meta.setdefault('retry_wait', 3)
        meta.setdefault('dupe_filter', False)

        return Tasker.make_task({
            'spider_name': self.name,
            'callback': n_callback,
            'request': request,
            'meta': meta,
            'priority': kwargs.get('priority', 0),
        })
