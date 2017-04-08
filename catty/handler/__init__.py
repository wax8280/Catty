#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/4/7 21:24
import traceback
from multiprocessing.connection import Client, Listener

from catty.config import CONFIG

MAX_TRY = 3


class HandlerClinet:
    def send(self, msg):
        for handler_type, port in CONFIG['PORT'].items():
            faild = True
            count = 0
            while faild and count < MAX_TRY:
                try:
                    print(port)
                    client = Client(('localhost', port), authkey=CONFIG['AUTHKEY'].encode('utf-8'))
                    client.send(msg)
                    faild = False
                except:
                    count += 1
                    traceback.print_exc()


class HandlerMixin:
    def pause(self, spider_name):
        print('pause {}'.format(spider_name))
        self.spider_started.remove(spider_name)
        self.spider_paused.add(spider_name)

    def __init__(self, handler_type):
        self.port = CONFIG['PORT'][handler_type]

    def handle(self, conn):
        try:
            while True:
                # (type, ctx)
                msg = conn.recv()
                if msg[0] == 'pause':
                    self.pause(msg[1])
        except EOFError:
            print('Connection closed')

    def echo_server(self, address, authkey):
        serv = Listener(address, authkey=authkey)
        while True:
            try:
                client = serv.accept()
                self.handle(client)
            except Exception:
                traceback.print_exc()

    def run_handler(self):
        self.echo_server(('', self.port), authkey=CONFIG['AUTHKEY'].encode('utf-8'))


if __name__ == '__main__':
    s = HandlerClinet()
    s.send(('pause', 'spider'))
