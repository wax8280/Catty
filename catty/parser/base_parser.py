#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/24 14:29
from catty.exception import Retry_current_task


class BaseParser(object):
    @staticmethod
    def retry_current_task():
        raise Retry_current_task
