# !/usr/bin/env python
# coding: utf-8

import logging
import sys
import os
from logging.handlers import WatchedFileHandler
from catty.config import CONFIG
from functools import partial


class BaseLog(object):
    logger_dict = {}

    @staticmethod
    def log(logger_name, message, level):
        if level == 'INFO':
            BaseLog.get_logger(logger_name).info(message)
        elif level == 'DEBUG':
            BaseLog.get_logger(logger_name).debug(message)
        elif level == 'ERROR':
            BaseLog.get_logger(logger_name).error(message)
        elif level == 'WARN':
            BaseLog.get_logger(logger_name).warning(message)

    @staticmethod
    def get_logger(logger_name):

        failed_path = os.path.join(CONFIG['LOG_PATH'], logger_name)
        if not os.path.exists(failed_path):
            os.makedirs(failed_path)

        if logger_name not in BaseLog.logger_dict:
            logger = logging.getLogger(logger_name)
            formatter = logging.Formatter(
                '[%(asctime)s][' + logger_name + '] %(message)s')
            file_handler = WatchedFileHandler(
                os.path.join(failed_path, logger_name + '.log'))
            file_handler.setFormatter(formatter)
            stream_handler = logging.StreamHandler(sys.stdout)
            stream_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            logger.addHandler(stream_handler)
            logger.setLevel(logging.DEBUG)
            BaseLog.logger_dict[logger_name] = logger
        return BaseLog.logger_dict[logger_name]


class Log(BaseLog):
    def __init__(self, logger_name):
        self.logger_name = logger_name
        self._logger_info = partial(BaseLog.log, logger_name=self.logger_name, level='INFO')
        self._logger_debug = partial(BaseLog.log, logger_name=self.logger_name, level='DEBUG')

    def log_it(self, message, level='INFO'):
        if level == 'INFO':
            self._logger_info(message=message)
        elif level == 'DEBUG':
            self._logger_debug(message=message)
        elif level == 'ERROR':
            self._logger_debug(message=message)
        elif level == 'WARN':
            self._logger_debug(message=message)
