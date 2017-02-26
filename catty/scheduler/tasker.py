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
        'url': str,
        'status': int,
        'meta': str,
        'priority': int,

        'downloader': {
            'method': str,
            'date': str or dict
            'headers': str,
            'timeout': int,
        },

        'scheduler': {
            'exetime': int,
        },

        'parser': {
            'parser_return': dict
        },

        'response': {
            'response_obj': Response,
            'status': int,
            'status_code': int,
        }

        'callbacks': list,      # bound method
        'item': dict,          # the dict return from paser func
    }

status :    0        NOTSTART

}
"""
NOTSTART = 0

NOW = 0
from catty.libs.url import *
from catty.libs.utils import *


class Tasker(object):

    def _make_task(self, d):
        spider_name = d['spider_name']
        url = format_url(d.get('url', ''))
        status = NOTSTART

        method = d.get('method', 'GET')
        body = format_body(d.get('data', ''))
        headers = d.get('headers')
        timeout = d.get('timeout', 30)
        meta = d.get('meta', '')

        exetime = NOW
        priority = 0
        callbacks = d['callbacks']

        tid = md5string(url + body)

        t = {
            'tid': tid,
            'spdier_name': spider_name,
            'url': url,
            'status': status,
            'meta': meta,
            'fetcher': {
                'method': method,
                'body': body,
                'headers': headers,
                'timeout': timeout,
            },
            'scheduler': {
                'exetime': exetime,
                'priority': priority,
            },
            'callbacks': callbacks,
        }

        return t

    def make(self, d):
        return self._make_task(d)

    def dump_task(self, task):
        return pickle.dumps(task)

    def load_task(self, dumped_task):
        return pickle.loads(dumped_task)
