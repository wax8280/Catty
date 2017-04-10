#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/4/8 19:24
from catty.handler import HandlerClinet

if __name__ == '__main__':
    s = HandlerClinet()
    # a = s.send('{"type":"start","spider_name":"TestSpider"}')
    a = s.send('{"type":"start","spider_name":"TestSpider2"}')
    # a = s.send('{"type":"set_speed","spider_name":"TestSpider2","spider_speed":1000}')
    # a=s.send('{"type":"list_spiders"}')
    print(a)
