#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/04/22 10:18

SCHEDULER_DOWNLOADER = 'scheduler_downloader'
DOWNLOADER_PARSER = 'downloader_parser'
PARSER_SCHEDULER = 'parser_scheduler'


class STATUS_CODE:
    OK = 0
    ARGS_ERROR = 100
    USER_ERROR = 200
    UNKNOW_ERROR = 400

    UPLOAD_ERROR = 501
