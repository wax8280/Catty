#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/26 10:21
from catty.libs.utils import build_url

# TODO
DEFAULT_HEADERS = ''
DEFAULT_TIMEOUT = 60

# request schema
"""
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

"""


class Request(object):
    # __slots__ = ['method', 'url', 'params', 'data', 'headers', 'auth', 'allow_redirects', 'proxy', 'proxy_auth',
    #              'timeout', 'dumped_request']

    def __init__(self, url, method='GET', params=None, data=None, headers=DEFAULT_HEADERS, auth=None,
                 allow_redirects=True, proxy=None, proxy_auth=None, timeout=None, **kwargs):
        self.method = method
        self.url = build_url(url)
        self.params = params
        self.data = data
        self.headers = headers
        # TODO build auth
        self.auth = auth
        self.allow_redirects = allow_redirects
        self.proxy = proxy
        # TODO build auth
        self.proxy_auth = proxy_auth
        self.timeout = timeout
        # TODO handle cookie in next version
        # self.cookies = kwargs.get('cookies', '')

        self.dumped_request = {}

    def __getitem__(self, item):
        return self.__getattribute__(item)

    def dump_request(self) -> dict:
        if self.dumped_request:
            return self.dumped_request

        self.dumped_request = {
            'url': self.url,
            'method': self.method,
            'headers': self.headers,
            'allow_redirects': self.allow_redirects,
        }

        if self.params:
            self.dumped_request.update({'params': self.params})
        if self.data:
            self.dumped_request.update({'data': self.data})
        if self.auth:
            self.dumped_request.update({'auth': self.auth})
        if self.proxy:
            self.dumped_request.update({'proxy': self.proxy})
        if self.proxy_auth:
            self.dumped_request.update({'proxy_auth': self.proxy_auth})
        if self.timeout:
            self.dumped_request.update({'timeout': self.timeout})
        return self.dumped_request

    def __str__(self):
        return str(Request.dump_request(self))

    def __repr__(self):
        return str(Request.dump_request(self))
