#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/04/22 09:21

# 当读取队列为满或空时的等待时间
LOAD_QUEUE_INTERVAL = 1
# SELECTOR扫描一次的时间间隔
SELECTOR_INTERVAL = 1

NUM_OF_PARSER_MAKE_TASK = 5
NUM_OF_SCHEDULER_MAKE_TASK = 5

# Spider的默认配置
SPIDER_DEFAULT = {
    # 默认速度
    'SPEED': 1,
    # BloomFilter的默认种子
    'SEEDS': ["HELLO", "WORLD", "CATTY", "PYTHON", "APPLE", "THIS", "THAT", "MY", "HI", "NOT"],
    # BloomFilter的分块
    'BLOCKNUM': 1
}

# 默认的HTTP请求头部
DEFAULT_HEADERS = {}

# Spider脚步的路径
SPIDER_PATH = './tests/spider'
# 日志文件的路径
LOG_PATH = './log'

"""
Log level
CRITICAL = 50
FATAL = CRITICAL
ERROR = 40
WARNING = 30
WARN = WARNING
INFO = 20
DEBUG = 10
NOTSET = 0
"""
LOG_LEVEL = 20

# 队列的默认配置
QUEUE = {
    # 最大队列长度
    'MAX_SIZE': 100000
}

# 持久化的配置
PERSISTENCE = {
    # 是否持久化Task
    'PERSIST_BEFORE_EXIT': True,
    # 持久化文件的路径
    'DUMP_PATH': './data/dump'
}

# Handler的端口配置
PORT = {
    'SCHEDULER': 38383,
    'PARSER': 38384
}

# Handler之间的进程通信的AuthKey
AUTHKEY = b'hello'

# Downloader的配置
DOWNLOADER = {
    # 全局链接并发数
    'CONN_LIMIT': 10000,
    # 单一域名最大并发数
    'LIMIT_PER_HOST': 10000,
    # 请求完成后强制关闭链接
    'FORCE_CLOSE': True
}

WEBUI = {
    'AUTH': True,
    'USER': 'test',
    'PW': 'test',
}
