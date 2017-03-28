#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/3/28 13:29
from catty.parser.base_parser import BaseParser
from catty.spider.base_spider import BaseSpider
from pyquery import PyQuery

class MyParser(BaseParser):
    def parser_content_page(self, response):
        pq = PyQuery(response.body)
        print(pq('title').text())
        return {'title': pq('title').text(), }

    def parser_list_page(self, response):
        pq = PyQuery(response.body)

        # article_url = [article_a_tab.attr.href for article_a_tab in pq('.post .post-title a').items()]
        article_url = [article_a_tab.attr.href for article_a_tab in pq('a').items()]
        next_page_url = pq('.next a').eq(0).attr.href
        return {'article_url': article_url, 'next_page_url': next_page_url}


default_headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'zh-CN,zh;q=0.8',
}


class Spider(BaseSpider, MyParser):
    name = 'TestSpider'

    def start(self):
        callbacks = [
            # {'parser': self.parser_content_page, },
            {'parser': self.parser_list_page, 'fetcher': [self.get_list, self.get_content]},
        ]

        return self.request(
            url='http://applehater.cn/',
            callback=callbacks,
            headers=default_headers,
        )

    def get_list(self, item):
        callbacks = [
            # {'parser': self.parser_content_page, },
            {'parser': self.parser_list_page, 'fetcher': [self.get_list, self.get_content], },
        ]

        return self.request(
            url=item['next_page_url'],
            callback=callbacks,
            headers=default_headers,
        )

    def get_content(self, item):
        return [
            self.request(
                url=url,
                callback=[{'parser': self.parser_content_page}],
                headers=default_headers,
            )
            for url in item['article_url']]


if __name__ == '__main__':
    pass
