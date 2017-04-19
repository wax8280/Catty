#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/4/7 14:11

import math
import time
import traceback
from asyncio import BaseEventLoop

from catty.exception import *
from catty.libs.log import Log
from catty.libs.utils import *
from catty.message_queue.redis_queue import get_task, push_task, AsyncRedisPriorityQueue
from catty.config import CONFIG
from catty import SELECTOR_INTERVAL, REQUEST_QUEUE_FORMAT, SELECTOR


class Selector:
    def __init__(self,
                 spider_speed: dict,
                 scheduler_downloader_queue: AsyncRedisPriorityQueue,
                 requests_queue: dict,
                 spider_stopped: set,
                 spider_paused: set,
                 spider_started: set,
                 spider_ready_start: set,
                 spider_todo: set,
                 loop: BaseEventLoop):
        self.logger = Log(SELECTOR)
        self.scheduler_downloader_queue = scheduler_downloader_queue
        self.requests_queue = requests_queue
        self.loop = loop

        # time from begin to now
        self.running_time = 0
        self.run_at = int(time.time())

        self.spider_stopped = spider_stopped
        self.spider_paused = spider_paused
        self.spider_started = spider_started
        self.spider_ready_start = spider_ready_start
        self.spider_todo = spider_todo
        self.all_spider_speed = {}
        self.spider_speed_reciprocal = {}

        self.init_speed(spider_speed)

    def init_speed(self, all_spider_speed: dict):
        """init all spider speed"""
        self.all_spider_speed = all_spider_speed
        self.spider_speed_reciprocal = {k: math.ceil(1 / v) for k, v in self.all_spider_speed.items()}

    def update_speed(self, spider_name: str, speed: int):
        """update a spider speed"""
        self.all_spider_speed.update({spider_name: speed})
        self.spider_speed_reciprocal.update({spider_name: math.ceil(1 / speed)})

    async def select_task(self):
        # TODO 时间粒度
        last_running_time = self.running_time
        self.running_time = int(time.time()) - self.run_at

        # FIXME dump paused spider should not in Selector
        for spider_name in self.spider_started | self.spider_paused:
            speed_reciprocal = self.spider_speed_reciprocal[spider_name]
            for each_diff_time in range(last_running_time, self.running_time):
                # time's up
                if each_diff_time % speed_reciprocal == 0:

                    # if speed bigger than 1,means that at last 1 request per sec.
                    if self.all_spider_speed[spider_name] > 1:
                        for i in range(self.all_spider_speed[spider_name]):
                            requests_q = self.requests_queue.get(REQUEST_QUEUE_FORMAT.format(spider_name))
                            if requests_q:
                                task = await get_task(requests_q)
                                if task:
                                    if task['spider_name'] in self.spider_started:
                                        await push_task(self.scheduler_downloader_queue, task, self.loop)
                                        self.logger.log_it('[select_task]{} tid:{}'.format(spider_name, task['tid']))
                                    elif task['spider_name'] in self.spider_paused:
                                        # 持久化
                                        dump_task(task, CONFIG['DUMP_PATH'], 'scheduler', task['spider_name'])
                                    elif task['spider_name'] in self.spider_stopped:
                                        # 抛弃
                                        pass
                    else:
                        requests_q = self.requests_queue.get(REQUEST_QUEUE_FORMAT.format(spider_name))
                        if requests_q:
                            task = await get_task(requests_q)
                            if task:
                                if task['spider_name'] in self.spider_started:
                                    await push_task(self.scheduler_downloader_queue, task, self.loop)
                                    self.logger.log_it('[select_task]{} tid:{}'.format(spider_name, task['tid']))
                                elif task['spider_name'] in self.spider_paused:
                                    # 持久化
                                    dump_task(task, CONFIG['DUMP_PATH'], 'scheduler', task['spider_name'])
                                elif task['spider_name'] in self.spider_stopped:
                                    # 抛弃
                                    pass

        await asyncio.sleep(SELECTOR_INTERVAL, loop=self.loop)
        self.loop.create_task(self.select_task())
