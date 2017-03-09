#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/24 13:21

import pickle

# task schema
"""
{
    'task': {
        'tid': str,
        'spider_name': str,
        'status': int,
        'priority': int,
        'meta':dict

        'request': Request_obj(a aiohttop request obj),
        'downloader': {
        },

        'scheduler': {
            'exetime': int,
        },

        'parser': {
            'item': dict,          # the dict return from paser func
        },

        'response': {
            'response_obj': Response,
            'status': int,
            'status_code': int,
        }

        'callbacks': list,      # bound method
    }

status :    0        NOTSTART

}
"""
NOTSTART = 0
STARTED = 1

NOW = 0
from catty.libs.utils import *


class Tasker(object):
    def _make_task(self, request):
        status = NOTSTART
        exetime = NOW
        spider_name = request['spider_name']
        priority = request['priority']
        callbacks = request['callbacks']
        meta = request['meta']

        tid = md5string(request['resuest']['url'] + str(request['request']['data']))

        t = {
            'tid': tid,
            'spdier_name': spider_name,
            'status': status,
            'priority': priority,
            'meta': meta,
            'request': request['request'],
            'downloader': {},
            'scheduler': {
                'exetime': exetime,
            },
            'parser': {},
            'response': {},
            'callbacks': callbacks,
        }

        return t

    def make(self, request):
        return self._make_task(request)

    def dump_task(self, task):
        return pickle.dumps(task)

    def load_task(self, dumped_task):
        return pickle.loads(dumped_task)
