#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/24 14:31

class par:
    def a(self):
        return self.b


class sp(par):
    def b(self):
        print('fxxk')


ins = sp()
print(ins.a())
print(ins.b)
