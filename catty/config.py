#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/04/22 09:21

LOAD_QUEUE_INTERVAL = 1
SELECTOR_INTERVAL = 1

REQUEST_QUEUE_FORMAT = "{}:requests"

SPIDER_DEFAULT = {
    'SPEED': 1
}

DEFAULT_HEADERS = {}

SPIDER_PATH = '../test/spider'
LOG_PATH = '../log'
DUMP_PATH = '../data/dump'

QUEUE = {
    'MAX_SIZE': 100000
}

PERSISTENCE = {
    'PERSIST_BEFORE_EXIT': False
}

PORT = {
    'SCHEDULER': 38383,
    'PARSER': 38384
}

AUTHKEY = b'hello'

DUPE_FILTER = {
    'SEEDS': ["HELLO", "WORLD", "CATTY", "PYTHON", "APPLE", "THIS", "THAT", "MY", "HI", "NOT"],
    'BLOCKNUM': 1
}

DOWNLOADER = {
    'CONN_LIMIT': 10000,
    'LIMIT_PER_HOST': 10000,
    'FORCE_CLOSE': True
}
