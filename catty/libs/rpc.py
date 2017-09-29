# !/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://wax8280.github.io
# Created on 2017/9/29 12:27

from xmlrpc.server import SimpleXMLRPCServer
from socketserver import ThreadingMixIn
from xmlrpc.client import ServerProxy


class ThreadXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

class XMLRPCClient(ServerProxy):
    pass


if __name__ == '__main__':
    def func(a):
        return a

    application = ThreadXMLRPCServer(('localhost', 8888))
    application.register_function(func,'func')
    application.serve_forever()


