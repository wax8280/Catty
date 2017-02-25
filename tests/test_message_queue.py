#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/25 10:30
import unittest

from catty.message_queue.redis_queue import RedisPriorityQueue


class Test(unittest.TestCase):
    def setUp(self):
        self.queue = RedisPriorityQueue(name='MySpider:parser_scheduler', db=0, maxsize=100)
        self.queue.clear()

    def test_put_nowait(self):
        self.queue.put_nowait({'test': 'testing1', 'priority': 0})
        self.queue.put_nowait({'test': 'testing2', 'priority': 1})
        self.queue.put_nowait({'test': 'testing3', 'priority': 2})
        self.assertEqual(
                self.queue.redis.zrange('MySpider:parser_scheduler', 0, -1),
                [
                    b'\x80\x03}q\x00(X\x04\x00\x00\x00testq\x01X\x08\x00\x00\x00testing3q\x02X\x08\x00\x00\x00priorityq\x03K\x02u.',
                    b'\x80\x03}q\x00(X\x04\x00\x00\x00testq\x01X\x08\x00\x00\x00testing2q\x02X\x08\x00\x00\x00priorityq\x03K\x01u.',
                    b'\x80\x03}q\x00(X\x04\x00\x00\x00testq\x01X\x08\x00\x00\x00testing1q\x02X\x08\x00\x00\x00priorityq\x03K\x00u.']
        )

        self.queue.redis.flushdb()

    def test_get_nowait(self):
        self.queue.put_nowait({'test': 'testing1', 'priority': 0})
        self.queue.put_nowait({'test': 'testing3', 'priority': 2})
        self.queue.put_nowait({'test': 'testing2', 'priority': 1})

        self.assertEqual(
                self.queue.qsize(), 3
        )

        self.assertEqual(
                self.queue.get_nowait(),
                {'test': 'testing3', 'priority': 2}
        )
        self.assertEqual(
                self.queue.get_nowait(),
                {'test': 'testing2', 'priority': 1}
        )
        self.assertEqual(
                self.queue.get_nowait(),
                {'test': 'testing1', 'priority': 0}
        )

        self.assertEqual(
                self.queue.qsize(), 0
        )


if __name__ == '__main__':
    unittest.main()
