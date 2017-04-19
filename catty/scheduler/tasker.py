#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/24 13:21

import pickle
from catty.libs.utils import PriorityDict, md5string
from catty import NOTSTART

# task schema
"""
{
    'task': {
        'tid': str,
        'spider_name': str,
        'status': int,
        'priority': int,
        'retried': int,
        'meta':dict
        {
            'retry': int                    The count of retry.Default: 0
            'retry_wait': int               Default: 3
            'dupe_filter': bool             Default: False
        }
        # dont_filter:  bool

        'request': Request_obj(a aiohttop request obj),
        {
            'method':str                    HTTP method
            'url':str                       URL
            'params':dict/str               string must be encoded(optional)
            'data':dict/bytes               to send in the body of the request(optional)
            'headers':dict                  HTTP Headers(optional)
            'auth':aiohttp.BasicAuth        an object that represents HTTP Basic Authorization (optional)
            'allow_redirects':bool
            'proxy':str                     Proxy URL(optional)
            'proxy_auth':aiohttp.BasicAuth  an object that represents proxy HTTP Basic Authorization (optional)
            'timeout':int                   a timeout for IO operations, 5min by default(option).Use None or 0 to disable timeout checks.
        }

        'downloader': {
        },

        'scheduler': {
        },

        'parser': {
            'item': dict,          # the dict return from paser func
        },

        'response': Response_obj,
        {
            'status':int                    HTTP status code
            'method':str                    HTTP method
            'cookies':SimpleCookie          HTTP cookies of response (Set-Cookie HTTP header)
            'headers':list                  HTTP headers of response as unconverted bytes, a sequence of (key, value) pairs.
            'content_type':str              Content-Type header
            'charset':str                   The value is parsed from the Content-Type HTTP header.Returns str like 'utf-8' or None if no Content-Type header present in HTTP headers or it has no charset information.

            # TODO make history
            # 'history':list
            # 'text':str                    response’s body decoded using specified encoding parameter.

            'body':bytes                    response’s body as bytes.
            'use_time':float                the time cost in request
        }

        'callback': list,      # bound method      {'fetcher':bound_method,'parser':bound_method,'result_pipeline':'bound_method}
    }

status :    0        NOTSTART

}
"""


class Tasker(object):
    def _make_task(self, request):
        status = NOTSTART
        spider_name = request['spider_name']
        priority = request['priority']
        callback = request['callback']
        meta = request['meta']

        tid = md5string(request['request']['url'] + str(request['request']['data']))

        return PriorityDict({
            'tid': tid,
            'spider_name': spider_name,
            'status': status,
            'priority': priority,
            'meta': meta,
            'request': request['request'],
            'downloader': {},
            'scheduler': {},
            'parser': {},
            'response': {},
            'callback': callback,
        })

    def make(self, request):
        return self._make_task(request)

    def dump_task(self, task):
        return pickle.dumps(task)

    def load_task(self, dumped_task):
        return pickle.loads(dumped_task)
