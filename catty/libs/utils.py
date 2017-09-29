#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/24 19:50
import hashlib
import pickle
import os
from furl import furl
import aiosqlite

md5string = lambda x: hashlib.md5(utf8(x)).hexdigest()


def utf8(string):
    """
    Make sure string is utf8 encoded bytes.
    """
    if isinstance(string, bytes):
        return string
    else:
        return string.encode('utf8')


def get_default(obj, name_or_index, default=''):
    if name_or_index.isdigit():
        try:
            result = obj[name_or_index]
        except:
            result = default
    else:
        try:
            result = obj[name_or_index]
        except:
            try:
                result = obj.__getattribute__(name_or_index)
            except:
                result = default
    return result


def build_url(url):
    """Build the actual URL to use."""
    f = furl(url)
    return f.url


class PriorityDict(dict):
    def __eq__(self, other):
        return self['priority'] == other['priority']

    def __lt__(self, other):
        return self['priority'] < other['priority']


class Task(PriorityDict):
    pass


def get_eventloop():
    try:
        import uvloop
        return uvloop.new_event_loop()
    except ImportError:
        import asyncio
        return asyncio.get_event_loop()


def dump_pickle_data(root, name, obj):
    p = pickle.dumps(obj)
    if not os.path.exists(root):
        os.mkdir(root)
    path = os.path.join(root, name)
    with open(path, 'wb') as f:
        f.write(p)


def load_pickle_data(root, name):
    path = os.path.join(root, name)
    if os.path.exists(path):
        with open(path, 'rb') as f:
            t = f.read()
            return pickle.loads(t)


async def dump_task(task, dump_path, dump_type, spider_name):
    """mkdir & save task in sqlite"""
    if not os.path.exists(os.path.join(dump_path, dump_type)):
        os.mkdir(os.path.join(dump_path, dump_type))

    path = os.path.join(os.path.join(dump_path, dump_type), spider_name)
    if not os.path.exists(path):
        async with aiosqlite.connect(path) as conn:
            await conn.execute('CREATE TABLE dump_task (task_data VARCHAR(99999))')
            await conn.execute('INSERT INTO dump_task (task_data) VALUES (?)', [pickle.dumps(task)])
            await conn.commit()
    else:
        async with aiosqlite.connect(path) as conn:
            await conn.execute('INSERT INTO dump_task (task_data) VALUES (?)', [pickle.dumps(task)])
            await conn.commit()
    return True


async def load_task(dump_path, dump_type, spider_name, delete=True):
    """load task in sqlite.Return a empty list if empty"""
    path = os.path.join(os.path.join(dump_path, dump_type), spider_name)
    if os.path.exists(path):
        async with aiosqlite.connect(path) as conn:
            cursor = await conn.execute('SELECT task_data FROM dump_task')
            result = [pickle.loads(i[0]) async for i in cursor]

        if delete:
            os.remove(path)
        return result


def set_safe_remove(s, k):
    try:
        s.remove(k)
    except KeyError:
        pass


if __name__ == '__main__':
    # print(build_url('http://你好.世界/ドメイン.テスト'))
    # print(build_url('https://www.baidu.com/s?wd=墨迹'))

    foo = PriorityDict({'priority': 10, 'name': 'foo'})
    bar = PriorityDict({'priority': 10, 'name': 'bar'})
    print(foo == bar)
    foobar = PriorityDict({'priority': 20, 'name': 'foobar'})
    print(foobar == bar)
    print(foobar > bar)
