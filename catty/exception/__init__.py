#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/26 18:37
import asyncio
import queue


class Retry_current_task(Exception):
    def __str__(self):
        return "Retry current task."


AsyncQueueEmpty = asyncio.queues.QueueEmpty
AsyncQueueFull = asyncio.queues.QueueFull
QueueEmpty = queue.Empty
QueueFull = queue.Full
