#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/4/7 21:24
import json
import traceback
from multiprocessing.connection import Client, Listener, Connection
from typing import TYPE_CHECKING

import catty.config
import catty.config
from catty.libs.log import Log
from catty.libs.utils import *

if TYPE_CHECKING:
    from catty.scheduler import Scheduler
    from catty.parser import Parser


class HandlerClient:
    """
    To connect with WebUI-Server and Scheduler&Parser
    
    The method return a dict:
    schema:
    {
        'code':int      # the status code
        'msg':str       # (option) if error,the msg must return to tell what's wrong.
    }
    """

    def __init__(self):
        self.logger = Log('HandlerClient')

    def _send(self, msg: str) -> dict:
        all_msg = {}
        for handler_type, port in catty.config.PORT.items():
            success = False
            while not success:
                try:
                    client = Client(('localhost', port), authkey=catty.config.AUTHKEY)
                    client.send(msg)
                    all_msg.update({handler_type: client.recv()})
                    success = True
                except Exception as e:
                    self.logger.log_it("[_send]ErrInfo:[}".format(e), 'WARN')
                    self.logger.log_it(traceback.format_exc(), 'WARN')
        return json.dumps(all_msg)

    def _send_scheduler(self, msg: str) -> dict:
        success = False
        port = catty.config.PORT['SCHEDULER']
        while not success:
            try:
                client = Client(('localhost', port), authkey=catty.config.AUTHKEY)
                client.send(msg)
                return client.recv()
            except Exception as e:
                self.logger.log_it("[_send_scheduler]ErrInfo:[}".format(e), 'WARN')
                self.logger.log_it(traceback.format_exc(), 'WARN')

    def _send_parser(self, msg: str) -> dict:
        success = False
        port = catty.config.PORT['PARSER']
        while not success:
            try:
                client = Client(('localhost', port), authkey=catty.config.AUTHKEY)
                client.send(msg)
                return client.recv()
            except Exception as e:
                self.logger.log_it("[_send_parser]ErrInfo:[}".format(e), 'WARN')
                self.logger.log_it(traceback.format_exc(), 'WARN')

    def index_list(self) -> dict:
        """Get the context which will use in index page"""
        spiders_list = json.loads(self._send_scheduler('{"type":"list_spiders"}'))
        spiders_count = json.loads(self._send_parser('{"type":"handle_count"}'))
        spiders_speed = json.loads(self._send_scheduler('{"type":"list_speed"}'))

        if spiders_list['code'] == 0 and spiders_count['code'] == 0 and spiders_speed['code'] == 0:
            result = {
                'code': 0,
                'Started': {},
                'Todo': {},
                'Paused': {},
                'Stopped': {},
                'Ready start': {},
                'All': {},
            }
            # Re-package
            for spider_type, spider_name_list in spiders_list['message'].items():
                for spider_name in spider_name_list:
                    spider = result[spider_type].setdefault(spider_name, {})
                    all_spider = result['All'].setdefault(spider_name, {})

                    if spider_name + '_success' in spiders_count['message']:
                        d = {'success_count': spiders_count['message'][spider_name + '_success'],
                             'fail_count': spiders_count['message'][spider_name + '_fail'],
                             }
                    else:
                        d = {'success_count': (0, 0, 0, 0),
                             'fail_count': (0, 0, 0, 0),
                             }

                    if spider_name in spiders_speed['message']:
                        d.update({'speed': spiders_speed['message'][spider_name]})
                    else:
                        d.update({'speed': 'NULL'})

                    d.update({'status': spider_type})

                    spider.update(d)
                    all_spider.update(d)
        else:
            result = {'code': -1, 'msg': (spiders_list, spiders_count, spiders_speed)}

        return result

    def action(self, action_type: str, spider_name: str, param: str = '') -> dict:
        """Handle action such as 'run','start','set speed'..."""
        if action_type == 'set_speed':
            result = json.loads(self._send_scheduler(json.dumps({
                'type': action_type, 'spider_name': spider_name, 'spider_speed': param})))
        elif action_type in ['start', 'run', 'pause', 'stop', 'clean_request_queue', 'update_spider', 'delete_spider']:
            result = json.loads(self._send(json.dumps({
                'type': action_type,
                'spider_name': spider_name
            })))
        else:
            result = {'code': 1, 'msg': 'Not a vaild action type.'}

        return result


class HandlerMixin:
    """
    A Handler to mixin Scheduler and Parser.

    Code:
        0   Normal
        -1  Error
        1   Warn
    """

    def __init__(self):
        self._logger = Log('HandlerMixin')

    def handle_pause_spider(self: "Scheduler", spider_name) -> tuple:
        """Pause the spider"""

        """
        Scheduler & Parser:
            Started->-spider_started & +spider_paused
            Paused->PASS
            Stop->PASS
        """
        code = 0
        msg = {}
        if spider_name in self.spider_started:
            set_safe_remove(self.spider_started, spider_name)
            self.spider_paused.add(spider_name)

        self._logger.log_it("[pause_spider]Success pause spider spider:{}".format(spider_name))

        # Dump request queue
        if self.name == 'Scheduler':
            self.loop.create_task(self.dump_persist_request_queue_task(spider_name))

        return code, msg

    def handle_stop_spider(self: "Scheduler", spider_name) -> tuple:
        """Stop the spider"""

        """
        Scheduler & Parser:
            Started->-spider_started & +spider_stopped
            Paused->-spider_paused & _spider_stopped
            Stopped->PASS
        """
        code = 0
        msg = {}
        if spider_name in self.spider_started:
            set_safe_remove(self.spider_started, spider_name)
            self.spider_stopped.add(spider_name)
        elif spider_name in self.spider_paused:
            set_safe_remove(self.spider_paused, spider_name)
            self.spider_stopped.add(spider_name)

        self._logger.log_it("[stop_spider]Success pause spider spider:{}".format(spider_name))
        return code, msg

    def handle_run_spider(self: "Scheduler", spider_name):
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
        code = 0
        msg = {}
        if spider_name in self.spider_paused:
            set_safe_remove(self.spider_paused, spider_name)
            self.spider_started.add(spider_name)
        elif spider_name in self.spider_stopped:
            set_safe_remove(self.spider_stopped, spider_name)
            if self.name == 'Scheduler':
                self.spider_ready_start.add(spider_name)
            else:
                self.spider_started.add(spider_name)
        elif self.name == 'Scheduler' and spider_name in self.spider_todo:
            set_safe_remove(self.spider_todo, spider_name)
            self.spider_started.add(spider_name)
        elif self.name == 'Parser':
            self.spider_started.add(spider_name)

        self._logger.log_it("[run_spider]Success spider:{}".format(spider_name))

        # load the persist file
        if self.name == 'Scheduler':
            self.loop.create_task(self.load_persist_request_queue_task(spider_name))

        return code, msg

    def handle_start_spider(self: "Scheduler", spider_name):
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
        code = 0
        msg = {}
        if self.name == 'Scheduler' and spider_name in self.spider_todo:
            set_safe_remove(self.spider_todo, spider_name)
            self.spider_ready_start.add(spider_name)
        elif spider_name in self.spider_paused:
            set_safe_remove(self.spider_paused, spider_name)
            self.spider_started.add(spider_name)
            if self.name == 'Scheduler':
                self.spider_ready_start.add(spider_name)
        elif spider_name in self.spider_stopped:
            set_safe_remove(self.spider_stopped, spider_name)
            self.spider_started.add(spider_name)
            if self.name == 'Scheduler':
                self.spider_ready_start.add(spider_name)
        elif spider_name in self.spider_started:
            if self.name == 'Scheduler':
                self.spider_ready_start.add(spider_name)
        elif self.name == 'Parser':
            self.spider_started.add(spider_name)

        self._logger.log_it("[start_spider]Success spider:{}".format(spider_name))

        # load the persist file
        if self.name == 'Scheduler':
            self.loop.create_task(self.load_persist_request_queue_task(spider_name))

        return code, msg

    # --------------------------------------------------------------------

    def handle_list_spiders(self: "Scheduler") -> tuple:
        return 0, {
            'Started': list(get_default(self, 'spider_started')) if get_default(self, 'spider_started') else [],
            'Todo': list(get_default(self, 'spider_todo')) if get_default(self, 'spider_todo') else [],
            'Paused': list(get_default(self, 'spider_paused')) if get_default(self, 'spider_paused') else [],
            'Stopped': list(get_default(self, 'spider_stopped')) if get_default(self, 'spider_stopped') else [],
            'Ready start': list(get_default(self, 'spider_ready_start')) if get_default(self,
                                                                                        'spider_ready_start') else []
        }

    def handle_list_speed(self: "Scheduler") -> tuple:
        return 0, self.selector.spider_speed

    def handle_count(self: "Parser") -> tuple:
        return 0, self.counter.count_all()

    # ---------------------------------------------------------------------
    def handle_set_speed(self: "Scheduler", spider_name: str, speed: int) -> tuple:
        if self.name == 'Scheduler':
            try:
                self.selector.update_speed(spider_name, int(speed))
            except Exception:
                return -1, {'msg': traceback.format_exc()}
        return 0, {}

    def handle_clean_request_queue(self: "Scheduler", spider_name: str) -> tuple:
        self.loop.create_task(self.clean_queue(spider_name))
        return 0, {}

    def handle_clean_dupe_filter(self: "Scheduler", spider_name: str) -> tuple:
        self.loop.create_task(self.clean_dupefilter(spider_name))
        return 0, {}

    def handle_update_spider(self: "Scheduler", spider_name: str) -> tuple:
        try:
            self.spider_module_handle.update_spider(spider_name)
            if '.py' in spider_name:
                self.spider_todo.add(getattr(self.spider_module_handle.namespace[spider_name][1], 'Spider').name)
                self.selector.update_speed(getattr(self.spider_module_handle.namespace[spider_name][1], 'Spider').name, 1)
        except Exception:
            return -1, {'msg': traceback.format_exc()}
        return 0, {}

    def handle_delete_spider(self: "Scheduler", spider_name: str) -> tuple:
        # TODO clean queue
        try:
            self.handle_clean_request_queue(spider_name)
            self.handle_clean_dupe_filter(spider_name)
            self.spider_module_handle.handle_delete_spider(spider_name)
        except Exception:
            return -1, {'msg': traceback.format_exc()}

        for spider_set in self.all_spider_set:
            if spider_name in spider_set:
                spider_set.remove(spider_name)

        return 0, {}

    # ---------------------------------------------------------------------

    def handle(self: "Scheduler", conn: Connection):
        try:
            while True:
                msg = json.loads(conn.recv())
                try:
                    r = -1, {}
                    if msg['type'] == 'pause':
                        r = self.handle_pause_spider(msg['spider_name'])
                    elif msg['type'] == 'start':
                        r = self.handle_start_spider(msg['spider_name'])
                    elif msg['type'] == 'run':
                        r = self.handle_run_spider(msg['spider_name'])
                    elif msg['type'] == 'stop':
                        r = self.handle_stop_spider(msg['spider_name'])

                    elif msg['type'] == 'update_spider':
                        r = self.handle_update_spider(msg['spider_name'])
                    elif msg['type'] == 'delete_spider':
                        r = self.handle_delete_spider(msg['spider_name'])

                    if self.name == 'Scheduler':
                        if msg['type'] == 'list_spiders':
                            r = self.handle_list_spiders()
                        elif msg['type'] == 'list_speed':
                            r = self.handle_list_speed()
                        elif msg['type'] == 'set_speed':
                            r = self.handle_set_speed(msg['spider_name'], msg['spider_speed'])
                        elif msg['type'] == 'clean_request_queue':
                            r = self.handle_clean_request_queue(msg['spider_name'])

                    if self.name == 'Parser':
                        if msg['type'] == 'handle_count':
                            r = self.handle_count()

                    send_msg = json.dumps({"code": r[0], "message": r[1]})
                except Exception:
                    send_msg = json.dumps({"code": -1, "msg": traceback.format_exc()})

                conn.send(send_msg)
        except EOFError:
            pass

    def echo_server(self, address: tuple, authkey: str):
        serv = Listener(address, authkey=authkey)
        while True:
            try:
                client = serv.accept()
                self.handle(client)
            except Exception:
                pass

    def run_handler(self: "Scheduler"):
        if self.name == 'Scheduler':
            self.echo_server(
                ('', catty.config.PORT['SCHEDULER']),
                authkey=catty.config.AUTHKEY)
        else:
            self.echo_server(
                ('', catty.config.PORT['PARSER']),
                authkey=catty.config.AUTHKEY)
