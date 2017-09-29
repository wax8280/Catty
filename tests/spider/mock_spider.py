#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/9/28 13:29
from catty.parser import BaseParser
from catty.spider import BaseSpider


class MyParser(BaseParser):
    def mock_parser_with_response(self, response):
        return {'text': response,'status':'parsered'}

    def mock_parser_with_task_response(self, response, task):
        return task


class Spider(BaseSpider, MyParser):
    name = 'mock_spider'

    def start(self):
        callbacks = [{'parser': self.mock_parser_with_response}]

        return self.request(
            url='http://www.baidu.com',
            callback=callbacks,
        )
