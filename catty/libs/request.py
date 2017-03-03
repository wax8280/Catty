#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/26 10:21
from catty.libs.url import build_url

# TODO
DEFAULT_HEADERS = ''
DEFAULT_TIMEOUT = 60


class Request(object):
    def __init__(self, **kwargs):
        self.url = build_url(kwargs['url'], kwargs.get('params', None))
        self.method = kwargs.get('method', 'GET')
        self.headers = kwargs.get('headers', DEFAULT_HEADERS)
        self.meta = kwargs.get('meta', '')
        # TODO build body
        self.body = kwargs.get('data', '')
        self.timeout = kwargs.get('timeout', DEFAULT_TIMEOUT)

    @staticmethod
    def dump_request(request_obj):
        return {
            'url': request_obj.url,
            'method': request_obj.method,
            'headers': request_obj.headers,
            'meta': request_obj.meta,
            'body': request_obj.body,
            'timeout': request_obj.timeout,
        }

    @staticmethod
    def load_request(d):
        return Request(d)

    def __str__(self):
        return str(Request.dump_request(self))


if __name__ == '__main__':
    r = Request(url='http://www.baidu.com/fxxk?start=10&end=20')
    print(str(r))
