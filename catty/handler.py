#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/4/7 21:24
from typing import TYPE_CHECKING

import catty.config
import catty.config
from catty.libs.log import Log
from catty.libs.utils import *
from catty.libs.rpc import ThreadXMLRPCServer, XMLRPCClient
from catty import DOWNLOADER_PARSER, PARSER_SCHEDULER, STATUS_CODE

if TYPE_CHECKING:
    from catty.scheduler import Scheduler
    from catty.parser import Parser


class BaseHandleClient:
    """ To connect with WebUI-Server and Scheduler&Parser"""
    logger = Log('HandlerClient')
    scheduler_handler_name = {'pause', 'start', 'run', 'stop', 'update_spider', 'delete_spider', 'list_spiders',
                              'list_speed', 'set_speed', 'clean_request_queue', 'clean_dupe_filter'}
    parser_handler_name = {'list_count', 'pause', 'start', 'run', 'stop', 'update_spider', 'delete_spider'}
    scheduler_parser_handler_name = scheduler_handler_name & parser_handler_name

    def __init__(self):
        self.scheduler_clients = {k: XMLRPCClient("http://localhost:{}".format(v))
                                  for k, v in catty.config.PORT['SCHEDULER'].items()}
        self.parser_clients = {k: XMLRPCClient("http://localhost:{}".format(v))
                               for k, v in catty.config.PORT['PARSER'].items()}

    def action(self, action_type: str, **kwargs) -> list:
        """Handle action such as 'run','start','set speed'..."""

        context = {'type': action_type}
        context.update(kwargs)

        results = []

        if action_type in self.scheduler_parser_handler_name:
            for _, each_parser_client in self.parser_clients.items():
                results.append(getattr(each_parser_client, action_type)(context))
            for _, each_scheduler_client in self.scheduler_clients.items():
                results.append(getattr(each_scheduler_client, action_type)(context))
        elif action_type in self.scheduler_handler_name:
            for _, each_scheduler_client in self.scheduler_clients.items():
                results.append(getattr(each_scheduler_client, action_type)(context))
        elif action_type in self.parser_handler_name:
            for _, each_parser_client in self.parser_clients.items():
                results.append(getattr(each_parser_client, action_type)(context))
        else:
            results = [{'code': STATUS_CODE.USER_ERROR, 'msg': 'Not a vaild action type.'}]

        return results


class HandlerClient(BaseHandleClient):
    def index_list(self) -> dict:
        """Get the context which will use in index page"""
        spider_list_status_code, spiders_list = getattr(self.scheduler_clients['master_scheduler'], 'list_spiders')({})

        spider_count_status_code, spiders_count = getattr(self.parser_clients['master_parser'], 'list_count')({})
        spider_speed_status_code, spiders_speed = getattr(self.scheduler_clients['master_scheduler'], 'list_speed')({})

        if spider_list_status_code == STATUS_CODE.OK and spider_speed_status_code == STATUS_CODE.OK \
                and spider_count_status_code == STATUS_CODE.OK:
            result = {
                'code': STATUS_CODE.OK,
                'Started': {},
                'Todo': {},
                'Paused': {},
                'Stopped': {},
                'Ready start': {},
                'All': {},
            }
            # Re-package
            for spider_type, spider_name_list in spiders_list.items():
                for spider_name in spider_name_list:
                    spider = result[spider_type].setdefault(spider_name, {})
                    all_spider = result['All'].setdefault(spider_name, {})

                    if spider_name + '_success' in spiders_count:
                        d = {'success_count': spiders_count[spider_name + '_success'],
                             'fail_count': spiders_count[spider_name + '_fail'],
                             }
                    else:
                        d = {'success_count': (0, 0, 0, 0),
                             'fail_count': (0, 0, 0, 0), }

                    if spider_name in spiders_speed:
                        d.update({'speed': spiders_speed[spider_name]})
                    else:
                        d.update({'speed': 'NULL'})

                    d.update({'status': spider_type})

                    spider.update(d)
                    all_spider.update(d)
        else:
            return {'code': STATUS_CODE.UNKNOW_ERROR, 'msg': (spiders_list, spiders_count, spiders_speed)}

        return result


class HandlerMixin:
    """ A Handler to mixin Scheduler and Parser. """

    def __init__(self):
        self._logger = Log('HandlerMixin')
        self.scheduler_handler = {
            'pause': self.handle_pause_spider, 'start': self.handle_start_spider, 'run': self.handle_run_spider,
            'stop': self.handle_stop_spider, 'update_spider': self.handle_update_spider,
            'delete_spider': self.handle_delete_spider, 'list_spiders': self.handle_list_spiders,
            'list_speed': self.handle_list_speed, 'set_speed': self.handle_set_speed,
            'clean_request_queue': self.handle_clean_request_queue, 'clean_dupe_filter': self.handle_clean_dupe_filter}
        self.parser_handler = {
            'list_count': self.handle_count, 'pause': self.handle_pause_spider, 'start': self.handle_start_spider,
            'run': self.handle_run_spider, 'stop': self.handle_stop_spider, 'update_spider': self.handle_update_spider,
            'delete_spider': self.handle_delete_spider}

        self.all_handler = {}
        self.all_handler.update(self.scheduler_handler)
        self.all_handler.update(self.parser_handler)

    def handle_pause_spider(self: "Scheduler", msg) -> tuple:
        """Pause the spider"""

        """
        Scheduler & Parser:
            Started->-spider_started & +spider_paused
            Paused->PASS
            Stop->PASS
        """
        spider_name = msg.get('spider_name')
        if not spider_name:
            return STATUS_CODE.ARGS_ERROR, {}

        if spider_name in self.spider_started:
            set_safe_remove(self.spider_started, spider_name)
            self.spider_paused.add(spider_name)

        # Dump request queue
        if 'scheduler' in self.name:
            self.loop.create_task(self.dump_tasks(spider_name))
        elif 'parser' in self.name:
            self.loop.create_task(self.dump_tasks(DOWNLOADER_PARSER))
            self.loop.create_task(self.dump_tasks(PARSER_SCHEDULER))

        self._logger.log_it("[pause_spider]Success pause spider spider:{}".format(spider_name))
        return STATUS_CODE.OK, {}

    def handle_stop_spider(self: "Scheduler", msg) -> tuple:
        """Stop the spider"""

        """
        Scheduler & Parser:
            Started->-spider_started & +spider_stopped
            Paused->-spider_paused & _spider_stopped
            Stopped->PASS
        """
        spider_name = msg.get('spider_name')
        if not spider_name:
            return STATUS_CODE.ARGS_ERROR, {}

        if spider_name in self.spider_started:
            set_safe_remove(self.spider_started, spider_name)
            self.spider_stopped.add(spider_name)
        elif spider_name in self.spider_paused:
            set_safe_remove(self.spider_paused, spider_name)
            self.spider_stopped.add(spider_name)

        self._logger.log_it("[stop_spider]Success pause spider spider:{}".format(spider_name))
        return STATUS_CODE.OK, {}

    def handle_run_spider(self: "Scheduler", msg):
        """continue if spider was paused or start from begin if spider was stopped"""

        """
        Scheduler:
            Started->PASS
            PAUSED->+spider_started
            STOPPED->+spider_ready_start
            TODO->+spider_started
        Parser:
            Started->PASS
            PAUSED->+spider_started
            STOPPED->+spider_started
        """
        spider_name = msg.get('spider_name')
        if not spider_name:
            return STATUS_CODE.ARGS_ERROR, {}

        if spider_name in self.spider_paused:
            set_safe_remove(self.spider_paused, spider_name)
            self.spider_started.add(spider_name)
        elif spider_name in self.spider_stopped:
            set_safe_remove(self.spider_stopped, spider_name)
            if 'scheduler' in self.name:
                self.spider_ready_start.add(spider_name)
            else:
                self.spider_started.add(spider_name)
        elif 'scheduler' in self.name and spider_name in self.spider_todo:
            set_safe_remove(self.spider_todo, spider_name)
            self.spider_started.add(spider_name)
        elif 'parser' in self.name:
            self.spider_started.add(spider_name)

        # load the persist file
        if 'scheduler' in self.name:
            self.loop.create_task(self.load_tasks(spider_name))
        else:
            self.loop.create_task(self.load_tasks(spider_name, PARSER_SCHEDULER))
            self.loop.create_task(self.load_tasks(spider_name, DOWNLOADER_PARSER))
        self._logger.log_it("[run_spider]Success spider:{}".format(spider_name))

        return STATUS_CODE.OK, {}

    def handle_start_spider(self: "Scheduler", msg):
        """start from begin if spider is todo,start from begin & contionue is spider was paused or stopped"""
        """
        Scheduler:
            Started->PASS
            PAUSED->+spider_ready_start
            STOPPED->+spider_ready_start
            TODO->+spider_ready_start
        Parser:
            Started->PASS
            PAUSED->+spider_started
            STOPPED->+spider_started
        """
        spider_name = msg.get('spider_name')
        if not spider_name:
            return STATUS_CODE.ARGS_ERROR, {}

        if 'scheduler' in self.name and spider_name in self.spider_todo:
            set_safe_remove(self.spider_todo, spider_name)
            self.spider_ready_start.add(spider_name)
        elif spider_name in self.spider_paused:
            set_safe_remove(self.spider_paused, spider_name)
            self.spider_started.add(spider_name)
            if 'scheduler' in self.name:
                self.spider_ready_start.add(spider_name)
        elif spider_name in self.spider_stopped:
            set_safe_remove(self.spider_stopped, spider_name)
            self.spider_started.add(spider_name)
            if 'scheduler' in self.name:
                self.spider_ready_start.add(spider_name)
        elif spider_name in self.spider_started:
            if 'scheduler' in self.name:
                self.spider_ready_start.add(spider_name)
        elif 'parser' in self.name:
            self.spider_started.add(spider_name)
        self._logger.log_it("[start_spider]Success spider:{}".format(spider_name))
        return STATUS_CODE.OK, {}

    # ------------------------SCHEDULER_ONLY----------------------------------

    def handle_list_spiders(self: "Scheduler", msg) -> tuple:
        return STATUS_CODE.OK, {
            'Started': list(get_default(self, 'spider_started')) if get_default(self, 'spider_started') else [],
            'Todo': list(get_default(self, 'spider_todo')) if get_default(self, 'spider_todo') else [],
            'Paused': list(get_default(self, 'spider_paused')) if get_default(self, 'spider_paused') else [],
            'Stopped': list(get_default(self, 'spider_stopped')) if get_default(self, 'spider_stopped') else [],
            'Ready start': list(get_default(self, 'spider_ready_start')) if get_default(self,
                                                                                        'spider_ready_start') else []
        }

    def handle_list_speed(self: "Scheduler", msg) -> tuple:
        return STATUS_CODE.OK, self.selector.spider_speed

    def handle_set_speed(self: "Scheduler", msg) -> tuple:
        spider_name = msg.get('spider_name')
        speed = msg['spider_speed']
        if 'scheduler' in self.name:
            self.selector.update_speed(spider_name, int(speed))
        return STATUS_CODE.OK, {}

    def handle_clean_request_queue(self: "Scheduler", msg) -> tuple:
        spider_name = msg.get('spider_name')
        if not spider_name:
            return STATUS_CODE.ARGS_ERROR, {}

        if self.name == 'master_scheduler':
            self.loop.create_task(self.clean_requests_queue(spider_name))
            return STATUS_CODE.OK, {}
        else:
            return STATUS_CODE.NOT_MY_BUSINESS, {}

    def handle_clean_dupe_filter(self: "Scheduler", msg) -> tuple:
        spider_name = msg.get('spider_name')
        if not spider_name:
            return STATUS_CODE.ARGS_ERROR, {}

        if self.name == 'master_parser':
            self.loop.create_task(self.clean_dupefilter(spider_name))
            return STATUS_CODE.OK, {}
        else:
            return STATUS_CODE.NOT_MY_BUSINESS, {}

    # ---------------------------------------------------------------------

    def handle_update_spider(self: "Scheduler", msg) -> tuple:
        spider_name = msg.get('spider_name')
        if not spider_name:
            return STATUS_CODE.ARGS_ERROR, {}

        self.spider_module_handle.update_spider(spider_name)
        # only can upload .py files
        if '.py' in spider_name:
            self.spider_todo.add(getattr(self.spider_module_handle.namespace[spider_name][1], 'Spider').name)
            self.selector.update_speed(getattr(self.spider_module_handle.namespace[spider_name][1], 'Spider').name, 1)
        return STATUS_CODE.OK, {}

    def handle_delete_spider(self: "Scheduler", msg) -> tuple:
        spider_name = msg.get('spider_name')
        if not spider_name:
            return STATUS_CODE.ARGS_ERROR, {}

        if spider_name in self.spider_started or ('scheduler' in self.name and spider_name in self.spider_ready_start):
            return STATUS_CODE.USER_ERROR, {'msg': "Please stop the spider first."}

        if 'scheduler' in self.name:
            self.handle_clean_request_queue(spider_name)
            self.handle_clean_dupe_filter(spider_name)

        self.spider_module_handle.delete_spider(spider_name)

        for spider_set in self.all_spider_set:
            if spider_name in spider_set:
                spider_set.remove(spider_name)

        self.loop.create_task(self.clean_requests_queue(spider_name))
        self.loop.create_task(self.clean_dupefilter(spider_name))
        return STATUS_CODE.OK, {}

    # -------------------------PARSER ONLY---------------------------------

    def handle_count(self: "Parser", msg) -> tuple:
        return STATUS_CODE.OK, self.counter.count_all()

    # ---------------------------------------------------------------------

    def xmlrpc_run(self: "Scheduler", name):
        if 'scheduler' in self.name:
            application = ThreadXMLRPCServer(('localhost', catty.config.PORT['SCHEDULER'][name]))
            for k, v in self.scheduler_handler.items():
                application.register_function(v, k)
        else:
            application = ThreadXMLRPCServer(('localhost', catty.config.PORT['PARSER'][name]))
            for k, v in self.parser_handler.items():
                application.register_function(v, k)

        application.serve_forever()
