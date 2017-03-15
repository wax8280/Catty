#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/24 13:25
from catty.parser.base_parser import BaseParser
from catty.spider.base_spider import BaseSpider

class MyParser(BaseParser):
    def parser_content_page(self, response):
        print('parser_content_page')
        content = 'content in parser_content_page'

        # must return a dict
        return {'content': content,}

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
        content_urls = 'content_urls in parser_content_page'
        content_titles = 'content_titles in parser_content_page'

        # must return a dict
        return {'content_url': content_urls, 'content_titles': content_titles}

    def save_content(self, response):
        print('saved')

    def save_list(self, response):
        print('saved')


class MySpider(BaseSpider, MyParser):
    name = 'MySpider'

    def start(self):
        callbacks = [
            {'parser': self.parser_list_page, 'fetcher': self.get_list, 'result_pipeline': self.save_list},
            {'parser': self.parser_content_page, 'fetcher': self.get_content},
        ]

        return self.request(
                url='http://blog.vincentzhong.cn/',
                method='GET',
                headers='',
                # meta to save somethings
                meta='',
                callback=callbacks,
        )

    def get_list(self, items):
        callbacks = [
            {'parser': self.parser_content_page, 'fetcher': self.get_content},
            {'parser': self.parser_list_page, 'fetcher': self.get_list, 'result_pipeline': self.save_list}
        ]
        return self.request(
                url=items['next_page'],
                callback=callbacks,
                # meta=items['meta'],
        )

    def get_content(self, items):
        requests = []
        for url, title in items[0]:
            requests.append(self.request(
                    url=url,
                    callback={'result_pipeline': self.save_content}
            ))

        return requests


if __name__ == '__main__':
    pass
