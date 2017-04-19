#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/4/18 13:38

import asynctest
from catty.libs.bloom_filter import RedisBloomFilter


class Test(asynctest.TestCase):
    use_default_loop = True

    @classmethod
    def setUpClass(cls):
        cls.redis_bloomfilter = RedisBloomFilter(loop=cls.loop, key="Test",
                                                 seeds=[
                                                     b"HELLO",
                                                     b"WORLD",
                                                     b"CATTY",
                                                     b"PYTHON",
                                                     b"APPLE",
                                                     b"THIS",
                                                     b"THAT",
                                                     b"MY",
                                                     b"HI",
                                                     b"NOT",
                                                 ])

    async def test(self):
        await self.redis_bloomfilter.conn()
        self.assertEqual(await self.redis_bloomfilter.add('fdd4f2b7cf28f018ba98af2510c227ee'), True)
        self.assertEqual(await self.redis_bloomfilter.add('world'), True)

        self.assertEqual(await self.redis_bloomfilter.is_contain('hello'), True)
        self.assertEqual(await self.redis_bloomfilter.is_contain('world'), True)

        self.assertEqual(await self.redis_bloomfilter.is_contain('new_world'), False)
        self.assertEqual(await self.redis_bloomfilter.is_contain('hi'), False)
        self.assertEqual(await self.redis_bloomfilter.is_contain('not not'), False)
