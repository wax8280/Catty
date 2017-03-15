#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/3/9 21:01

# response schema
"""
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
"""


class Response(object):
    def __init__(self, status, method, cookies, headers, content_type, charset, body, use_time):
        self.status = status
        self.method = method
        self.headers = headers
        self.cookies = cookies
        self.content_type = content_type
        self.charset = charset
        self.body = body
        self.use_time = use_time

        self.dumped_request = {}


    def __getitem__(self, item):
        return self.__getattribute__(item)

    def dump_request(self):
        if self.dumped_request:
            return self.dumped_request

        self.dumped_request = {
            'status': self.status,
            'method': self.method,
            'headers': self.headers,
            'body': self.body,
            'use_time': self.use_time
        }

        if self.cookies:
            self.dumped_request.update({'cookies': self.cookies})
        if self.content_type:
            self.dumped_request.update({'content_type': self.content_type})
        if self.charset:
            self.dumped_request.update({'charset': self.charset})

        return self.dumped_request

    def __str__(self):
        return str(Response.dump_request(self))
