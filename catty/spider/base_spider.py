#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/24 14:26
from catty.libs.request import Request


class BaseSpider(object):
    def request(self, **kwargs):
        request = Request(**kwargs)
        _d = {
            'spider_name': self.name,
            'request': request,
        }
        if 'priority' not in _d.keys():
            _d.update({'priority': 0})
        return _d
