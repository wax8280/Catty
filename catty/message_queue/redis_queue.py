#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/24 9:53

import queue
import time

import redis
import umsgpack


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
        self.redis.execute_command('ZADD', self.name, -obj.priority, umsgpack.packb(obj))
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
        if results is None:
            raise self.Empty
        return umsgpack.unpackb(results)

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