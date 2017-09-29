#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/3/28 13:29
import os

import aiofiles
from pyquery import PyQuery

from catty.parser import BaseParser
from catty.spider import BaseSpider
from catty.libs.utils import md5string

default_headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'zh-CN,zh;q=0.8',
}


async def write_response(root_path, text):
    async with aiofiles.open(os.path.join(root_path, md5string(text)), mode='wb') as f:
        await f.write(text)


class MyParser(BaseParser):
    def retry(self, task):
        if task['response']['status'] != 302:
            return task

    async def parser_content_page(self, response, task, loop):
        pq = PyQuery(response.body)
        urls = [a.attr.href for a in pq('a').items() if a.attr.href.startswith('http')]
        print(pq('title').text() + '\t' + str(task['meta']['deep']))
        await write_response('/mnt2/test', response.body)
        return {'urls': urls}

    def return_a_list(self, response):
        tasks = []
        "to make some task.only return a list can be treated as task"
        return tasks


class Spider(BaseSpider, MyParser):
    name = 'test_spider'
    urls = ['https://web.sogou.com/', 'http://www.999.com/xinwen/', 'http://www.haoqq.com/', 'http://news.hao123.com/']

    def start(self):
        callbacks = [{'parser': self.parser_content_page, 'fetcher': self.get_content}]

        return [self.request(
            url=url,
            callback=callbacks,
            headers=default_headers,
            meta={'deep': 100, 'dupe_filter': True}
        ) for url in self.urls]

    def get_content(self, task):
        callbacks = [{'parser': self.parser_content_page, 'fetcher': self.get_content}]

        return [
            self.request(
                url=url,
                callback=callbacks,
                headers=default_headers,
                meta={'deep': task['meta']['deep'] - 1, 'dupe_filter': True},
                priority=task['meta']['deep'] - 1,
            )
            for url in task['parser']['item']['urls']]
