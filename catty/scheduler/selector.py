#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/4/7 14:11

import math
import time
import traceback

from catty.exception import *
from catty.libs.log import Log

LOAD_QUEUE_SLEEP = 1
REQUEST_QUEUE_FORMAT = "{}:requests"


class Selector:
    def __init__(self, spider_speed, scheduler_downloader_queue, requests_queue, loop):
        self.logger = Log('Parser')
        self.scheduler_downloader_queue = scheduler_downloader_queue
        self.requests_queue = requests_queue
        self.loop = loop

        self.running_time = 0
        self.running_at = int(time.time())

        self.update_speed(spider_speed)

    def update_speed(self, spider_speed):
        self.spider_speed = spider_speed
        self.spider_speed_reciprocal = {k: math.ceil(1 / v) for k, v in self.spider_speed.items()}

    async def load_task(self, spider_name):
        """load item from parser-scheduler queue"""
        try:
            requests_q = self.requests_queue.get(REQUEST_QUEUE_FORMAT.format(spider_name))
            if requests_q:
                # get from requests queue,push to scheduler-downloader queue
                task = await requests_q.get()
                self.logger.log_it(message="[load_task]Load item", level='INFO')
                return task
            else:
                self.logger.log_it("[load_task]No {} requests queue.".format(spider_name), 'WARN')
        except AsyncQueueEmpty:
            # traceback.print_exc()
            return
        except Exception:
            traceback.print_exc()
            return

    async def push_task(self, q, item):
        """ push task to scheduler-downloader queue"""

        done = False
        while not done:
            try:
                await q.put(item)
                done = True
            except AsyncQueueFull:
                traceback.print_exc()
                # print('[push_task]Redis queue is full')
                await asyncio.sleep(LOAD_QUEUE_SLEEP, loop=self.loop)
            except Exception as e:
                traceback.print_exc()
                await asyncio.sleep(LOAD_QUEUE_SLEEP, loop=self.loop)

        self.logger.log_it(message="[push_task]Push item", level='INFO')
        return True

    async def select_task(self):
        # TODO 时间粒度
        last_running_time = self.running_time
        self.running_time = int(time.time()) - self.running_at
        for spider_name, speed_reciprocal in self.spider_speed_reciprocal.items():
            for each_diff_time in range(last_running_time, self.running_time):
                if each_diff_time % speed_reciprocal == 0:
                    if self.spider_speed[spider_name] > 1:
                        for i in range(self.spider_speed[spider_name]):
                            task = await self.load_task(spider_name)
                            if task:
                                await self.push_task(self.scheduler_downloader_queue, task)
                                self.logger.log_it('[select_task]push {}'.format(spider_name))
                    else:
                        task = await self.load_task(spider_name)
                        if task:
                            await self.push_task(self.scheduler_downloader_queue, task)
                            self.logger.log_it('[select_task]push {}'.format(spider_name))

        await asyncio.sleep(1, loop=self.loop)
        self.loop.create_task(self.select_task())


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    s = Selector([], '', '', loop)

    s.update_speed({'1/s': 1, '0.3/s': 0.3, '3/s': 3})
    loop.create_task(s.select_task())
    loop.run_forever()
