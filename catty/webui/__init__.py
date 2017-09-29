#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/4/11 18:14
import os

from functools import wraps
from flask import render_template, Response, Flask, json, request

import catty.config
from catty import STATUS_CODE
from catty.handler import HandlerClient

app = Flask(__name__)
app.debug = True
client = HandlerClient()


def check_auth(username, password):
    return username == catty.config.WEBUI['USER'] and password == catty.config.WEBUI['PW']


def authenticate():
    """Sends a 401 response that enables basic auth"""
    return Response(
        'Could not verify your access level for that URL.\n'
        'You have to login with proper credentials', 401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'})


def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if catty.config.WEBUI['AUTH'] and not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)

    return decorated


@app.route('/upload', methods=['GET', 'POST'])
@requires_auth
def upload_file():
    if request.method == 'POST':
        try:
            f = request.files['py_script']
            f.save(os.path.join(catty.config.SPIDER_PATH, f.filename))
        except Exception as e:
            return json.dumps({'status_code': STATUS_CODE.UPLOAD_ERROR, 'msg': str(e)})

        status_code, context = client.action("update_spider")
        if status_code == STATUS_CODE.OK:
            return json.dumps({'status_code': STATUS_CODE.OK})
        else:
            return json.dumps({'status_code': STATUS_CODE.UNKNOW_ERROR, 'msg': 'Error in `update_spider`'})
    else:
        return json.dumps({'status_code': STATUS_CODE.USER_ERROR, 'msg': 'Not POST method!'})


@app.route('/action', methods=['GET'])
@requires_auth
def action_page():
    action_type = request.args.get('action_type', '')
    kwagrs = {'spider_name': request.args.get('spider_name')}
    if action_type:
        return str(client.action(action_type, **kwagrs))


@app.route('/')
@requires_auth
def index_page():
    result = client.index_list()
    return render_template('index.html', result=result)


if __name__ == '__main__':
    app.run()
