#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/24 9:53

import pickle
import queue
import time
from asyncio import queues as async_queue
import traceback

import aioredis
import redis

from catty.libs.utils import get_default
from catty.exception import *
from catty import *


class BaseQueue(object):
    # 引用标准库queue中的Exception
    Empty = queue.Empty
    Full = queue.Full

    def __init__(self, name, host='localhost', port=6379, db=0, password=None):
        """
        :param name:        队列的名字
        :param host:        redis所在主机
        :param port:        端口号
        :param db:          db
        :param password:    redis密码
        :return:
        """
        self.name = name
        self.redis = redis.StrictRedis(host=host, port=port, db=db, password=password)

    def put(self, request):
        raise NotImplementedError

    def get(self, timeout=0):
        raise NotImplementedError

    def clear(self):
        self.redis.delete(self.name)


class RedisPriorityQueue(BaseQueue):
    """
    使用redis的优先级队列，使用redis的有序集合
    """

    # 非block的get与put中的失败等待时间
    max_timeout = 0.3

    def __init__(self, name, host='localhost', port=6379, db=0,
                 maxsize=0, password=None):
        """
        :param maxsize:     队列的最大上限
        :return:
        """
        super(RedisPriorityQueue, self).__init__(name, host, port, db, password)
        self.maxsize = maxsize
        self.last_qsize = 0

    def __len__(self):
        return self.qsize()

    def qsize(self):
        self.last_qsize = self.redis.zcard(self.name)
        return self.last_qsize

    def empty(self):
        if self.qsize() == 0:
            return True
        else:
            return False

    def full(self):
        if self.maxsize and self.qsize() >= self.maxsize:
            return True
        else:
            return False

    def put_nowait(self, obj):
        if self.full():
            raise self.Full
        # umsgpack用于序列化数据
        # TODO 每个obj必须有一个priority属性
        priority = -get_default(obj, 'priority', 0)
        self.redis.execute_command('ZADD', self.name, priority, pickle.dumps(obj))
        return True

    def put(self, obj, block=True, timeout=None):
        if not block:
            return self.put_nowait()

        start_time = time.time()
        while True:
            try:
                return self.put_nowait(obj)
            except self.Full:
                if timeout:
                    lasted = time.time() - start_time
                    if timeout > lasted:
                        time.sleep(min(self.max_timeout, timeout - lasted))
                    else:
                        raise
                else:
                    time.sleep(self.max_timeout)

    def get_nowait(self):
        pipe = self.redis.pipeline()
        pipe.multi()
        # 取出并删除最高优先级的元素
        pipe.zrange(self.name, 0, 0).zremrangebyrank(self.name, 0, 0)
        results, count = pipe.execute()
        if not results:
            raise self.Empty
        return pickle.loads(results[0])

    def get(self, block=True, timeout=None):
        if not block:
            return self.get_nowait()

        start_time = time.time()
        while True:
            try:
                return self.get_nowait()
            except self.Empty:
                if timeout:
                    lasted = time.time() - start_time
                    if timeout > lasted:
                        time.sleep(min(self.max_timeout, timeout - lasted))
                    else:
                        raise
                else:
                    time.sleep(self.max_timeout)


class BaseAsyncQueue(object):
    # 引用标准库queue中的Exception
    Empty = async_queue.QueueEmpty
    Full = async_queue.QueueFull

    def __init__(self, name, loop, host='localhost', port='6379', db=0, password=None, pool_maxsize=10):
        """
        :param name:        队列的名字
        :param loop:        EventLoop
        :param host:        redis所在主机
        :param port:        端口号
        :param db:          db
        :param password:    redis密码
        :return:
        """
        self.name = name
        self.loop = loop

        self.host = host
        self.port = port
        self.db = db
        self.pw = password

        self.redis_pool = None
        self.redis_conn = None
        self.pool_maxsize = pool_maxsize

    def __repr__(self):
        return self.name

    async def conn_pool(self):
        if not self.redis_pool:
            self.redis_pool = await aioredis.create_pool(
                (self.host, self.port), db=self.db, loop=self.loop, maxsize=self.pool_maxsize
            )

        return self.redis_pool

    async def conn(self):
        if not self.redis_conn:
            self.redis_conn = await aioredis.create_redis(
                (self.host, self.port), db=self.db, loop=self.loop
            )
        return self.redis_conn

    async def put(self, item):
        raise NotImplementedError

    async def get(self):
        raise NotImplementedError

    async def clear(self):
        await self.redis_conn.delete(self.name)


class AsyncRedisPriorityQueue(BaseAsyncQueue):
    def __init__(self, name, loop, host='localhost', port=6379, db=0,
                 queue_maxsize=10000, password=None, pool_maxsize=10):
        """
        :param queue_maxsize:     队列的最大上限
        :return:
        """
        super(AsyncRedisPriorityQueue, self).__init__(name, loop, host, port, db, password, pool_maxsize)
        self.queue_maxsize = queue_maxsize
        self.last_qsize = 0
        self.loop = loop

    async def qsize(self):
        try:
            self.last_qsize = await self.redis_conn.zcard(self.name)
            return self.last_qsize
        except:
            await self.conn()
            self.loop.create_task(self.qsize())

    async def empty(self):
        qsize = await self.qsize()
        if qsize == 0:
            return True
        else:
            return False

    async def full(self):
        if self.queue_maxsize and await self.qsize() >= self.queue_maxsize:
            return True
        else:
            return False

    async def get(self):
        # pipe = self.redis_conn.pipeline()
        tr = self.redis_conn.multi_exec()
        tr.zrange(self.name, 0, 0)
        tr.zremrangebyrank(self.name, 0, 0)
        result, count = await tr.execute()
        if not result:
            raise self.Empty

        return pickle.loads(result[0])

    async def put(self, item):
        is_full = await self.full()
        if is_full:
            raise self.Full

        if isinstance(item, tuple):
            priority = -item[0]
            item = item[1]
        else:
            priority = -get_default(item, 'priority', 0)
        await self.redis_conn.zadd(self.name, priority, pickle.dumps(item))
        return True


async def get_task(q):
    """
    Get a task from queue.
    :param q:       Redis-Queue
    :return:        Task
    """
    try:
        t = await q.get()
        return t
    except AsyncQueueEmpty:
        return
    except Exception:
        traceback.print_exc()
        await q.conn()
        return


async def push_task(q, task, loop):
    """
    Push a task to ququq
    :param q:       Redis-Queue
    :param task:    Task
    """
    done = False
    while not done:
        try:
            await q.put(task)
            done = True
        except AsyncQueueFull:
            await asyncio.sleep(LOAD_QUEUE_SLEEP, loop=loop)
        except Exception as e:
            traceback.print_exc()
            await q.conn()
            await asyncio.sleep(LOAD_QUEUE_SLEEP, loop=loop)
