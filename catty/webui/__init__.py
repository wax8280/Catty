#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/4/11 18:14
import os

from flask import Flask
from flask import json
from flask import render_template
from flask import request

import catty.config
from catty.handler import HandlerClient

app = Flask(__name__)
app.debug = True
client = HandlerClient()


@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        try:
            f = request.files['py_script']
            f.save(os.path.join(catty.config.SPIDER_PATH, f.filename))
            client.action("update_spider", f.filename)
        except Exception as e:
            return json.dumps({'code': -1, 'msg': str(e)})
    return json.dumps({'code': 0, 'msg': 'Upload Success!'})


@app.route('/action', methods=['GET'])
def action_page():
    action_type = request.args.get('action_type', '')
    spider_name = request.args.get('spider_name', '')
    param = request.args.get('param', '')

    print(action_type, spider_name, param)
    if action_type:
        result = client.action(action_type, spider_name, param)

    return str(result)


@app.route('/')
def index_page():
    result = client.index_list()
    print(result)
    return render_template('index.html', result=result)


if __name__ == '__main__':
    app.run()
