#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Vincent<vincent8280@outlook.com>
#         http://blog.vincentzhong.cn
# Created on 2017/2/24 19:57
from urllib.parse import urlparse,urlunparse
from requests.models import RequestEncodingMixin

def encode_multipart_formdata(fields, files):
    body, content_type = RequestEncodingMixin._encode_files(files, fields)
    return content_type, body

_encode_params = RequestEncodingMixin._encode_params

def build_url(url, _params=None):
    """Build the actual URL to use."""
    # TODO encode the chinese url

    # Support for unicode domain names and paths.
    scheme, netloc, path, params, query, fragment = urlparse(url)
    netloc = netloc.encode('idna').decode('utf-8')
    if not path:
        path = '/'

    if _params:
        enc_params = _encode_params(_params)
        if enc_params:
            if query:
                query = '%s&%s' % (query, enc_params)
            else:
                query = enc_params

    url = (urlunparse([scheme, netloc, path, params, query, fragment]))
    return url

def quote_chinese(url, encodeing="utf-8"):
    """Quote non-ascii characters"""
    if isinstance(url, str):
        return quote_chinese(url.encode(encodeing))

    res = [bytes(b).decode('latin-1') if b < 128 else '%%%02X' % b for b in url]
    return "".join(res)

def format_body(data: str or dict):
    """make request body"""
    pass
    return data