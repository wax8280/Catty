#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/24 9:51
NOTSTART = 0
STARTED = 1
READY_TO_START = 2
STOP = 3
PAUSE = 4

LOAD_QUEUE_SLEEP = 1
DUMP_PERSIST_FILE_INTERVAL = 10
SELECTOR_SLEEP = 3
REQUEST_QUEUE_FORMAT = "{}:requests"

SELECTOR_INTERVAL = 1