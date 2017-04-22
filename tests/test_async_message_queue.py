#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/3/8 19:35
import asynctest

from catty.message_queue import AsyncRedisPriorityQueue


class Test(asynctest.TestCase):
    use_default_loop = True

    async def setUp(self):
        self.queue = AsyncRedisPriorityQueue(
            name='MySpider:parser_scheduler', loop=self.loop, db=0, queue_maxsize=100)
        await self.queue.conn()
        await self.queue.clear()

    async def tearDown(self):
        await self.queue.clear()

    async def test_put(self):
        await self.queue.put({'test': 'testing1', 'priority': 0})
        await self.queue.put({'test': 'testing2', 'priority': 1})
        await self.queue.put({'test': 'testing3', 'priority': 2})
        self.assertEqual(
            await self.queue.redis_conn.zrange('MySpider:parser_scheduler', 0, -1),
            [
                b'\x80\x03}q\x00(X\x04\x00\x00\x00testq\x01X\x08\x00\x00\x00testing3q\x02X\x08\x00\x00\x00priorityq\x03K\x02u.',
                b'\x80\x03}q\x00(X\x04\x00\x00\x00testq\x01X\x08\x00\x00\x00testing2q\x02X\x08\x00\x00\x00priorityq\x03K\x01u.',
                b'\x80\x03}q\x00(X\x04\x00\x00\x00testq\x01X\x08\x00\x00\x00testing1q\x02X\x08\x00\x00\x00priorityq\x03K\x00u.']
        )

        await self.queue.clear()

    async def test_get(self):
        await self.queue.put({'test': 'testing1', 'priority': 0})
        await self.queue.put({'test': 'testing3', 'priority': 2})
        await self.queue.put({'test': 'testing2', 'priority': 1})

        self.assertEqual(
            await self.queue.qsize(), 3
        )

        self.assertEqual(
            await self.queue.get(),
            {'test': 'testing3', 'priority': 2}
        )
        self.assertEqual(
            await self.queue.get(),
            {'test': 'testing2', 'priority': 1}
        )
        self.assertEqual(
            await self.queue.get(),
            {'test': 'testing1', 'priority': 0}
        )

        self.assertEqual(
            await self.queue.qsize(), 0
        )


if __name__ == '__main__':
    asynctest.main()
