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
        self.url = build_url(kwargs['url'])
        self.method = kwargs.get('method', 'GET')
        self.headers = kwargs.get('headers', DEFAULT_HEADERS)
        self.meta = kwargs.get('meta', '')
        self.data = kwargs.get('data', '')
        self.timeout = kwargs.get('timeout', DEFAULT_TIMEOUT)

    def __str__(self):
        return {
            'url': self.url,
            'method': self.method,
            'headers': self.headers,
            'meta': self.meta,
            'data': self.data,
            'timeout': self.timeout,
        }


if __name__ == '__main__':
    r = Request(url='htt[://www.baidu.com/fxxk?start=10&end=20')
    print(str(r))
