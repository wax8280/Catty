#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/25 10:30

import unittest

from catty.message_queue.redis_queue import RedisPriorityQueue
from catty.scheduler.scheduler import Scheduler


class Test(unittest.TestCase):
    def setUp(self):
        self.scheduler_downloader_queue = RedisPriorityQueue('MySpider:SD')
        self.parser_scheduler_queue = RedisPriorityQueue('MySpider:PS')

        self.scheduler = Scheduler(
                self.scheduler_downloader_queue,
                self.parser_scheduler_queue,
        )

    def test_spider_start_method(self):
        from catty.demo.spider import MySpider
        self.scheduler.instantiate_spdier(spider_class=MySpider, spider_name='MySpider')
        self.scheduler.make_task_from_start()


        self.assertIn(
                "'spider_name': 'MySpider'",
                self.scheduler._task_to_downloader[0]
        )

    def test_make_tasks(self):
        from catty.demo.spider import MySpider
        self.scheduler.instantiate_spdier(spider_class=MySpider, spider_name='MySpider')
        self.scheduler.make_task_from_start()

        # assume the task downloaded
        task = self.scheduler._task_to_downloader.pop()
        # parser中，有多少个callback line就复制多少个 task
        # 因为不同的parser生成不同的item，不同的item生成不同的task
        task['callbacks'] = task['callbacks'][0]
        item = task['callbacks']['parser'](response='')
        task.update({'item': item})
        self.scheduler._task_from_parser.append(task)

        self.scheduler.make_tasks()
        print(self.scheduler._task_to_downloader)

        self.assertIn(
            "'url': '....'",
            str(self.scheduler._task_to_downloader[0]),
        )

    def test_load_item(self):
        self.parser_scheduler_queue.put_nowait({'test1': 'testing1'})
        self.parser_scheduler_queue.put_nowait({'test2': 'testing2'})
        self.parser_scheduler_queue.put_nowait({'test3': 'testing3'})

        self.scheduler.load_item()
        self.assertEqual(
                self.scheduler._task_from_parser,
                [{'test1': 'testing1'}, {'test2': 'testing2'}, {'test3': 'testing3'}]
        )

    def test_load_item_limit_pre_loop(self):
        self.scheduler._task_from_parser = []
        self.scheduler._count_pre_loop = 1
        self.parser_scheduler_queue.put_nowait({'test1': 'testing1'})
        self.parser_scheduler_queue.put_nowait({'test2': 'testing2', 'priority': 1})
        self.parser_scheduler_queue.put_nowait({'test3': 'testing3'})

        self.scheduler.load_item()
        self.assertEqual(
                self.scheduler._task_from_parser,
                [{'test2': 'testing2', 'priority': 1}]
        )


if __name__ == '__main__':
    unittest.main()
