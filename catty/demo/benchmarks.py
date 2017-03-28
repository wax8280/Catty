# #!/usr/bin/env python
# # -*- encoding: utf-8 -*-
# # vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# # Author: Vincent<vincent8280@outlook.com>
# #         http://blog.vincentzhong.cn
# # Created on 2017/3/28 13:29
# from catty.parser.base_parser import BaseParser
# from catty.spider.base_spider import BaseSpider
# from pyquery import PyQuery
# import json
# import time
#
# c = 1
#
#
# class MyParser(BaseParser):
#     def parser_list_page(self, response):
#         global c
#         print(c)
#         c += 1
#         return {'content': response.body}
#
#
# class Spider(BaseSpider, MyParser):
#     name = 'TestSpider'
#
#     def start(self):
#         callbacks = [
#             {'parser': self.parser_list_page, 'fetcher': [self.get_list]},
#         ]
#
#         return self.request(
#             url='http://127.0.0.1:8000/',
#             callback=callbacks,
#         )
#
#     def get_list(self, item):
#         callbacks = [
#             {'parser': self.parser_list_page, 'fetcher': [self.get_list], },
#         ]
#
#         return [self.request(
#             url='http://127.0.0.1:8000/' + str(time.time()) + str(i),
#             callback=callbacks,
#         ) for i in range(100)]
#
#
# if __name__ == '__main__':
#     pass
