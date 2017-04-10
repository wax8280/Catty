#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/4/7 21:24
import traceback
from multiprocessing.connection import Client, Listener

from catty.config import CONFIG
from catty import *
from catty.libs.log import Log
from catty.libs.utils import *
import json

MAX_TRY = 3


class HandlerClinet:
    def send(self, msg):
        for handler_type, port in CONFIG['PORT'].items():
            faild = True
            count = 0
            while faild and count < MAX_TRY:
                try:
                    client = Client(('localhost', port), authkey=CONFIG['AUTHKEY'].encode('utf-8'))
                    client.send(msg)
                    print(client.recv())
                    faild = False
                except:
                    count += 1

    def send_scheduler(self, msg):
        faild = True
        count = 0
        port = CONFIG['PORT']['SCHEDULER']
        while faild and count < MAX_TRY:
            try:
                client = Client(('localhost', port), authkey=CONFIG['AUTHKEY'].encode('utf-8'))
                client.send(msg)
                print(client.recv())
                faild = False
            except:
                count += 1


class HandlerMixin:
    # --------------------------------------------------------------------
    def __init__(self):
        self.handler_log = Log('Hanlder')

    def pause_spider(self, spider_name):
        if spider_name in self.spider_started:
            set_safe_remove(self.spider_started, spider_name)
            self.spider_paused.add(spider_name)
            self.handler_log.log_it("[pause_spider]Success pause spider spider:{}".format(spider_name))
            return 0, '', ''
        else:
            self.handler_log.log_it(
                "[pause_spider]Failed pause spider spider:{}.Errinfo:Can't find spider in spider_started set.".format(
                    spider_name))
            return -1, ''

    def stop_spider(self, spider_name):
        if spider_name in self.spider_started:
            set_safe_remove(self.spider_started, spider_name)
            self.spider_stopped.add(spider_name)
            self.handler_log.log_it("[stop_spider]Success pause spider spider:{}".format(spider_name))
            return 0, ''
        else:
            self.handler_log.log_it(
                "[stop_spider]Failed pause spider spider:{}.Errinfo:Can't find spider in spider_started set.".format(
                    spider_name))
            return -1, ''

    def run_spider(self, spider_name):
        """continue if spider was paused or start from begin if spider was stopped"""

        """
        Scheduler:
            PAUSED->+spider_started
            STOPPED->+spider_ready_start
            TODO->+spider_started
        Parser:
            PAUSED->+spider_started
            STOPPED->+spider_started
        """
        code = 0
        msg = ''
        if spider_name in self.spider_paused:
            set_safe_remove(self.spider_paused, spider_name)
            self.spider_started.add(spider_name)
            self.handler_log.log_it("[run_spider]Success run spider spider:{}".format(spider_name))
        elif spider_name in self.spider_stopped:
            set_safe_remove(self.spider_stopped, spider_name)
            if self.name == 'Scheduler':
                self.spider_ready_start.add(spider_name)
            else:
                self.spider_started.add(spider_name)
            self.handler_log.log_it("[run_spider]Success run spider spider:{}".format(spider_name))
        elif self.name == 'Scheduler' and spider_name in self.spider_todo:
            set_safe_remove(self.spider_todo, spider_name)
            self.spider_started.add(spider_name)
            self.handler_log.log_it("[run_spider]Success run spider spider:{}".format(spider_name))
        elif self.name == 'Parser':
            self.spider_started.add(spider_name)
            self.handler_log.log_it("[run_spider]Success run spider spider:{}".format(spider_name))
        else:
            self.handler_log.log_it(
                "[run_spider]Failed run spider spider:{}.Errinfo:Can't find spider in spider_paused or spider_stopped or spider_todo set.".format(
                    spider_name))
            code = -1
            msg = ''

        if self.name == 'Scheduler':
            self.loop.create_task(self.load_task())

        return code, msg

    def start_spider(self, spider_name):
        """start from begin if spider is todo,start from begin & contionue is spider was paused or stopped"""
        code = 0
        msg = ''
        if self.name == 'Scheduler' and spider_name in self.spider_todo:
            set_safe_remove(self.spider_todo, spider_name)
            self.spider_ready_start.add(spider_name)
            self.handler_log.log_it("[start_spider]Success start spider spider:{}".format(spider_name))
        elif spider_name in self.spider_paused:
            set_safe_remove(self.spider_paused, spider_name)
            self.spider_started.add(spider_name)
            if self.name == 'Scheduler':
                self.spider_ready_start.add(spider_name)
            self.handler_log.log_it("[start_spider]Success start spider spider:{}".format(spider_name))
        elif spider_name in self.spider_stopped:
            set_safe_remove(self.spider_stopped, spider_name)
            self.spider_started.add(spider_name)
            if self.name == 'Scheduler':
                self.spider_ready_start.add(spider_name)
            self.handler_log.log_it("[start_spider]Success start spider spider:{}".format(spider_name))
        elif self.name != 'Scheduler':
            self.spider_started.add(spider_name)
            self.handler_log.log_it("[start_spider]Success start spider spider:{}".format(spider_name))
        else:
            self.handler_log.log_it(
                "[start_spider]Failed start spider spider:{}.Errinfo:Can't find spider in spider_paused or spider_stopped or spider_todo set.".format(
                    spider_name))
            code = -1
            msg = ''
        if self.name == 'Scheduler':
            self.loop.create_task(self.load_task())

        return code, msg

    # --------------------------------------------------------------------
    def list_todo_spiders(self):
        return 0, self.spider_todo

    def list_paused_spiders(self):
        return 0, self.spider_paused

    def list_stopped_spiders(self):
        return 0, self.spider_stopped

    def list_started_spiders(self):
        return 0, self.spider_started

    def list_ready_start_spiders(self):
        return 0, self.spider_ready_start

    def list_spiders(self):
        return 0, {
            'Todo': get_default(self, 'spider_todo'),
            'Paused': get_default(self, 'spider_paused'),
            'Stopped': get_default(self, 'spider_stopped'),
            'Ready start': get_default(self, 'spider_ready_start')
        }

    # ---------------------------------------------------------------------
    def set_speed(self, spider_name, speed):
        if self.name == 'Scheduler':
            self.selector.update_speed(spider_name, speed)
        return 0, ''

    def handle(self, conn):
        try:
            while True:
                # (type, ctx)
                msg = json.loads(conn.recv())
                try:
                    r = -1, ''
                    print(msg)
                    if msg['type'] == 'pause':
                        r = self.pause_spider(msg['spider_name'])
                    elif msg['type'] == 'start':
                        r = self.start_spider(msg['spider_name'])
                    elif msg['type'] == 'run':
                        r = self.run_spider(msg['spider_name'])
                    elif msg['type'] == 'stop':
                        r = self.stop_spider(msg['spider_name'])

                    if self.name == 'Scheduler':
                        if msg['type'] == 'list_spiders':
                            r = self.list_spiders()
                        elif msg['type'] == 'list_todo':
                            r = self.list_todo_spiders()
                        elif msg['type'] == 'list_start':
                            r = self.list_started_spiders()
                        elif msg['type'] == 'list_pause':
                            r = self.list_paused_spiders()
                        elif msg['type'] == 'list_stop':
                            r = self.list_stopped_spiders()
                        elif msg['type'] == 'set_speed':
                            r = self.set_speed(msg['spider_name'], msg['spider_speed'])

                    if r:
                        send_msg = json.dumps({"code": r[0], "message": str(r[1])})
                except Exception as e:
                    traceback.print_exc()
                    send_msg = '{"code":-1,"message":"%s"}' % str(e)

                conn.send(send_msg)
        except EOFError:
            print('Connection closed')

    def echo_server(self, address, authkey):
        serv = Listener(address, authkey=authkey)
        while True:
            try:
                client = serv.accept()
                self.handle(client)
            except Exception:
                pass

    def run_handler(self):
        if self.name == 'Scheduler':
            self.echo_server(('', CONFIG['PORT']['SCHEDULER']), authkey=CONFIG['AUTHKEY'].encode('utf-8'))
        else:
            self.echo_server(('', CONFIG['PORT']['PARSER']), authkey=CONFIG['AUTHKEY'].encode('utf-8'))


if __name__ == '__main__':
    s = HandlerClinet()
    s.send('{"type":"start","spider_name":"TestSpider"}')
