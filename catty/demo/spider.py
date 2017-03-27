#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/24 13:25
from catty.parser.base_parser import BaseParser
from catty.spider.base_spider import BaseSpider


class MyParser(BaseParser):
    @staticmethod
    def test_static():
        print('test_static')

    def test(self):
        print('parser testing')

    def parser_content_page(self, response):
        print('parser_content_page')
        content = 'content in parser_content_page'

        # must return a dict
        return {'content': content, }

        # if need,you can return a task(manual retry)
        # self.retry_current_task()

        # TODO
        # or you can return a request(not recommend)
        # because MyParser is a mixin class,if can use its parent's method
        # callbacks = [
        #     {'parser': self.parser_content_page, 'fetcher': self.get_content},
        #     {'parser': self.parser_list_page, 'fetcher': self.get_list, 'result_pipeline': self.save_list}
        # ]
        # return self.request(
        #         url='',
        #         callback=callbacks,
        #         meta='',
        # )

    def parser_list_page(self, response):
        print('parser_list_page')

        # must return a dict
        return {'content_url': ['url1', 'url2'], 'next_page': 'next_page_url'}


class Spider(BaseSpider, MyParser):
    name = 'MySpider'

    def start(self):
        callbacks = [
            {'parser': self.parser_list_page, 'fetcher': [self.get_list, self.get_content]},
            {'parser': self.parser_content_page, },
        ]

        return self.request(
            url='http://blog.vincentzhong.cn/',
            callback=callbacks,
        )

    def get_list(self, item):
        callbacks = [
            {'parser': self.parser_content_page, },
            {'parser': self.parser_list_page, 'fetcher': [self.get_list, self.get_content], }
        ]
        return self.request(
            url=item['next_page'],
            callback=callbacks,
        )

    def get_content(self, item):
        return [self.request(
            url=url,
            callback=[{'parser': self.parser_content_page}]
        ) for url in item['content_url']]


if __name__ == '__main__':
    pass
