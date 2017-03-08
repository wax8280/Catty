# !/usr/bin/env python
# coding: utf-8
import aiohttp
import asyncio
from queue import Queue
import threading
import time
from copy import deepcopy
import async_timeout
from asyncio.queues import PriorityQueue as AsynPriorityQueue
from asyncio.queues import Queue as AsynQueue
from aiohttp import BaseConnector
import uvloop

loop = uvloop.new_event_loop()
asyncio.set_event_loop(loop)
conn = aiohttp.TCPConnector(limit=50)


class Crawler:
    @staticmethod
    async def request(aio_request: dict, loop, connector: BaseConnector, out_q: AsynQueue):
        async with aiohttp.request(**aio_request, loop=loop, connector=connector) as client:
            await out_q.put({
                'response': {
                    'text': await client.text(),
                    'status': client.status,
                    'cookies': client.cookies,
                    'headers': client.headers,
                    'charset': client.charset,
                    'history': client.history,
                    'body': await client.read(),
                }
            })


async def get_from_queue(in_q: AsynPriorityQueue, out_q: AsynQueue):
    url = await in_q.get()
    loop.create_task(Crawler.request(aio_request={'url': url, 'method': 'GET'}, loop=loop, connector=conn, out_q=out_q))
    loop.create_task(get_from_queue(in_q, out_q))


async def print_result(q: AsynQueue):
    last = 0
    while True:
        print("{} item/s".format(q.qsize() - last))
        last = q.qsize()
        await asyncio.sleep(1)


in_q = AsynPriorityQueue()
for i in range(1000000):
    in_q.put_nowait('http://127.0.0.1:8000')
    # in_q.put_nowait('http://www.5566.net')

out_q = AsynQueue()
loop.create_task(get_from_queue(in_q, out_q))
loop.create_task(print_result(out_q))
loop.run_forever()
