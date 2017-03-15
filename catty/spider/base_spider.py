#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/24 14:26
from catty.libs.request import Request


class BaseSpider(object):
    def request(self, **kwargs):
        # to mixin
        request = Request(**kwargs)
        callback = kwargs['callback']
        # just str
        n_callback = [{k: v.__name__ for k, v in each_callback.items()} for each_callback in callback]

        _d = {
            'spider_name': self.name,
            'callback': n_callback,
            'request': request,
            'meta': kwargs.get('meta', {}),
            'priority': kwargs.get('priority', 0),
        }
        return _d


if __name__ == '__main__':
    from catty.demo.spider import Spider

    myspider = Spider()
    task=myspider.start()

    print(myspider.__getattribute__(task['callback'][0]['parser']))