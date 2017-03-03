# !/usr/bin/env python
# coding: utf-8
import aiohttp
import asyncio


class Crawler(object):
    @staticmethod
    def make_aio_request(url, method='GET'):
        return aiohttp.request(
            method=method,
            url=url,
        )

    @staticmethod
    async def get(aio_request):
        async with aio_request as resp:
            return resp


k = []


async def get(url):
    print(url)
    async with aiohttp.get(url) as client:
        k.append(await client.text())


loop = asyncio.get_event_loop()
tasks = [get('http://blog.vincentzhong.cn?a={}'.format(i)) for i in range(10)]
loop.run_until_complete(asyncio.wait(tasks))
print(k)
