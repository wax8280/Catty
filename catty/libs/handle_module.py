#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/3/15 21:39
import importlib.util
import os
from catty.libs.log import Log


def load_module(script_path, module_name):
    """
    return a module
    spec.loader.exec_module(foo)
    foo.A()
    :param script_path:
    :param module_name:
    :return:
    """
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    module = importlib.util.module_from_spec(spec)
    return spec, module


def exec_module(spec, module):
    """use loader to exec the module"""
    spec.loader.exec_module(module)


def reload_module(module):
    """reload the module"""
    importlib.reload(module)


def search_in_dir(path, name):
    return set(file for file in os.listdir(path) if name in file)


class ModuleHandle:
    def __init__(self, path):
        self.path = path

        self.spider_module_filename = set()
        self.loaded_module_filename = set()

        # file_name:spec,module
        self.namespace = {}

    def load_new_module(self):
        """Load the module which no in self.namespace"""
        self.spider_module_filename = search_in_dir(self.path, '.py')

        for each_module_filename in self.spider_module_filename:
            if each_module_filename not in self.namespace:
                path = os.path.join(self.path, each_module_filename)
                spec, module = load_module(path, each_module_filename)
                exec_module(spec, module)
                self.namespace.update({each_module_filename: (spec, module)})
                self.loaded_module_filename.add(each_module_filename)
            else:
                # had load
                pass

        to_remove = set()
        for each_loaded_module_filename in self.loaded_module_filename:
            if each_loaded_module_filename not in self.spider_module_filename:
                self.namespace.pop(each_loaded_module_filename)
                to_remove.add(each_loaded_module_filename)
        self.loaded_module_filename -= to_remove

    def load_all_module(self):
        """reload all moduel"""
        self.spider_module_filename = search_in_dir(self.path, '.py')

        for each_not_loaded_module_filename in self.spider_module_filename:
            path = os.path.join(self.path, each_not_loaded_module_filename)
            spec, module = load_module(path, each_not_loaded_module_filename)
            exec_module(spec, module)

            self.namespace.update({each_not_loaded_module_filename: (spec, module)})
            self.loaded_module_filename.add(each_not_loaded_module_filename)

        # 删除
        to_remove = set()
        for each_loaded_module_filename in self.loaded_module_filename:
            if each_loaded_module_filename not in self.spider_module_filename:
                self.namespace.pop(each_loaded_module_filename)
                to_remove.add(each_loaded_module_filename)
        self.loaded_module_filename -= to_remove

    def update_module(self, spider_file_name):
        # TODO the Spider.name == filename
        if '.py' not in spider_file_name:
            spider_file_name = spider_file_name + '.py'
        path = os.path.join(self.path, spider_file_name)

        spec, module = load_module(path, spider_file_name)
        exec_module(spec, module)

        self.namespace.update({spider_file_name: (spec, module)})
        self.loaded_module_filename.add(spider_file_name)

    def delete_module(self, spider_file_name):
        if '.py' not in spider_file_name:
            spider_file_name = spider_file_name + '.py'

        path = os.path.join(self.path, spider_file_name)
        self.namespace.pop(spider_file_name)
        self.loaded_module_filename.remove(spider_file_name)
        if os.path.exists(path):
            os.remove(path)


class SpiderModuleHandle(ModuleHandle):
    def __init__(self, path):
        super().__init__(path)

        # spider_name:spider_instantiation
        self.spider_instantiation = {}
        self.logger = Log('SpiderModuleHandle')

    def instance_spider(self, name):
        try:
            spec, module = self.namespace[name]
            spider_cls = getattr(module, 'Spider')
            self.spider_instantiation.update({
                spider_cls.name: spider_cls()
            })
            self.logger.log_it("[load_spider]Load spider name:{}".format(spider_cls.name), 'INFO')
        except Exception as e:
            self.logger.log_it("[load_spider]ErrInfo:{}".format(e), 'WARN')

    def instance_all_spider(self):
        for file_name in self.namespace.keys():
            self.instance_spider(file_name)

    def load_all_spider(self):
        self.load_all_module()
        self.instance_all_spider()

    def load_new_spider(self):
        self.load_new_module()
        self.instance_all_spider()

    def update_spider(self, spider_file_name):
        if '.py' not in spider_file_name:
            spider_file_name = spider_file_name + '.py'
        self.update_module(spider_file_name)
        self.instance_spider(spider_file_name)

    def delete_spider(self, spider_file_name):
        # TODO spider_file_name == Spider.name
        self.spider_instantiation.pop(spider_file_name)
        self.delete_module(spider_file_name)


if __name__ == '__main__':
    spider_module_handle = SpiderModuleHandle('../../test_handle_module')
    spider_module_handle.load_all_spider()
    print(spider_module_handle.spider_instantiation)
