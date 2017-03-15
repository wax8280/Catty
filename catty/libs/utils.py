#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/24 19:50
import hashlib
from furl import furl
import importlib.util

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


def load_module(script_path, module_name):
    """
    return a module
    spec.loader.exec_module(foo)
    foo.A()
    :param script_path:
    :param module_name:
    :return:
    """
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    foo = importlib.util.module_from_spec(spec)
    return spec, foo


def exec_module(spec, module):
    """use loader to exec the module"""
    spec.loader.exec_module(module)


def reload_module(module):
    """reload the module"""
    importlib.reload(module)


if __name__ == '__main__':
    # print(build_url('http://你好.世界/ドメイン.テスト'))
    # print(build_url('https://www.baidu.com/s?wd=墨迹'))


    foo = PriorityDict({'priority': 10, 'name': 'foo'})
    bar = PriorityDict({'priority': 10, 'name': 'bar'})
    print(foo == bar)
    foobar = PriorityDict({'priority': 20, 'name': 'foobar'})
    print(foobar == bar)
    print(foobar > bar)
