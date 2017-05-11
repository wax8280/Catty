#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/4/18 10:38
import hashlib
import aioredis
from catty.libs.utils import md5string


class Hash(object):
    def __init__(self, size, seed):
        self.size = size
        self.seed = seed
        self.make_hashfunc()

    def make_hashfunc(self):
        seed = []
        for string in self.seed:
            if isinstance(string, str):
                string = string.encode('utf-8')
            else:
                string = str(string).encode('utf-8')

            seed.append(string)
        self.seed = seed
        self.hashfunc = [hashlib.md5(seed) for seed in self.seed]

    def hash(self, value):
        if isinstance(value, str):
            value = value.encode('utf-8')
        else:
            value = str(value).encode('utf-8')

        ret = []
        for hashfunc in self.hashfunc:
            h = hashfunc.copy()
            h.update(value)
            ret.append(int(h.hexdigest(), 16) % self.size)

        return ret


class RedisBloomFilter(object):
    def __init__(self, loop, key, seeds, host='localhost', port=6379, db=0, blockNum=1):
        """
        When error_rate=0.001(0.1%),100,000,000 data cost 125MB.
        :param loop:
        :param key:
        :param seeds:
        :param host:
        :param port:
        :param db:
        :param blockNum:
        """
        self.host = host
        self.port = port
        self.db = db
        self.loop = loop
        self.redis_conn = None

        self.bit_size = 1 << 29  # Redis的String类型最大容量为512M，现使用64M
        self.seeds = seeds
        self.key = key
        self.blockNum = blockNum
        self.hashfunc = Hash(self.bit_size, self.seeds)

    async def conn(self):
        if not self.redis_conn:
            self.redis_conn = await aioredis.create_redis(
                (self.host, self.port), db=self.db, loop=self.loop
            )
        return self.redis_conn

    async def clean(self):
        for i in range(self.blockNum):
            key = self.key + ':' + str(i)
            try:
                await self.redis_conn.delete(key)
            except:
                pass

    async def is_contain(self, string):
        if isinstance(string, str):
            string = string.encode('utf-8')
        else:
            string = str(string).encode('utf-8')

        # m_string = md5string(string)
        m_string = string
        ret = 1
        name = self.key + ':' + str(int(m_string, 16) % self.blockNum)
        loc = self.hashfunc.hash(m_string)

        for i in loc:
            a = await self.redis_conn.getbit(name, i)
            ret = ret & a
        return True if ret == 1 else False

    async def add(self, string):
        if isinstance(string, str):
            string = string.encode('utf-8')
        else:
            string = str(string).encode('utf-8')

        # m_string = md5string(string)
        m_string = string
        name = self.key + ':' + str(int(m_string, 16) % self.blockNum)
        loc = self.hashfunc.hash(m_string)

        pipe = self.redis_conn.pipeline()
        for i in loc:
            pipe.setbit(name, i, 1)
        r = await pipe.execute()

        return True if 1 not in r else False
