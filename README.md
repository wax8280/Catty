# Catty 简介

Catty是一个以Python3标准库[asyncio](https://www.python.org/dev/peps/pep-3156)与[await](https://www.python.org/dev/peps/pep-0492/),[async](https://www.python.org/dev/peps/pep-0492/)语法为基础编写的一个异步爬虫框架。

- 纯Python语言实现
- 支持Windows，Linux，Mac平台
- 具有WebUI的爬虫管理功能



## Catty安装

下列的安装步骤假定您已经安装好下列程序

- Python3.4+
- pip与setuptools

### Windows、Linux与Mac 平台安装

```
pip install -r requirement
```

## Catty运行

```
python ./test/run_downloader.py			# 运行Downloader
python ./test/run_parser.py				# 运行Parser
python ./test/run_scheduler.py			# 运行Scheduler
python ./test/webui.py					# 运行Webui
```

浏览器访问**http://127.0.0.1:5000**即可管理当前爬虫。

## Catty配置

Catty的配置文件为`./catty/config.py`。

```python
# 当读取队列为满或空时的等待时间
LOAD_QUEUE_INTERVAL = 1
# SELECTOR扫描一次的时间间隔
SELECTOR_INTERVAL = 1

# Spider的默认配置
SPIDER_DEFAULT = {
    # 默认速度
    'SPEED': 1,
    # BloomFilter的默认种子，有多少个种子即代表有多少个哈希函数
    'SEEDS': ["HELLO", "WORLD", "CATTY", "PYTHON", "APPLE", "THIS", "THAT", "MY", "HI", "NOT"],
    # BloomFilter的分块
    'BLOCKNUM': 1
}

# 默认的HTTP请求头部
DEFAULT_HEADERS = {}

# Spider脚步的路径
SPIDER_PATH = './test/spider'
# 日志文件的路径
LOG_PATH = './log'

# 队列的默认配置
QUEUE = {
    # 最大队列长度
    'MAX_SIZE': 100000
}

# 持久化的配置
PERSISTENCE = {
    # 是否持久化Task
    'PERSIST_BEFORE_EXIT': False,
    # 持久化文件的路径
    'DUMP_PATH': './data/dump'
}

# Handler的端口配置
PORT = {
    'SCHEDULER': 38383,
    'PARSER': 38384
}

# Handler之间的进程通信的AuthKey
AUTHKEY = b'hello'

# Downloader的配置
DOWNLOADER = {
    # 全局链接并发数
    'CONN_LIMIT': 10000,
    # 单一域名最大并发数
    'LIMIT_PER_HOST': 10000,
    # 请求完成后强制关闭链接
    'FORCE_CLOSE': True
}
```



## 代码样例

```python
from catty.parser import BaseParser
from catty.spider import BaseSpider

from catty.libs.utils import md5string
from pyquery import PyQuery
import aiofiles


class MyParser(BaseParser):
    def parser_index_page(self, response):
        pq = PyQuery(response.body)

        return {'detail_url': [a.attr.href for a in pq('a[href^="http"]').items()]}

    async def save_detail_page(self, response, loop):
        async with aiofiles.open(md5string(response.body), mode='wb') as f:
            await f.write(response.body)


class Spider(BaseSpider, MyParser):
    name = 'simple'
    def start(self):
        callback = {'parser': self.parser_index_page, 'fetcher': self.detail_page}
        return self.request(
            url='http://scrapy.org/',
            callback=callback
        )

    def detail_page(self, task):
        callback = {'parser': self.save_detail_page, }
        return [
            self.request(
                url=each_url,
                callback=callback
            )
            for each_url in task['parser']['item']['detail_url']
            ]
```

每个爬虫脚本需包含两个类

- 继承于`catty.parser.BaseParser`的类
- 继承于`catty.spider.BaseSpider`与上一个继承于`catty.parser.BaseParser`的类

第一个类称之为**解析类**，即专门编写用于解析与保存结果的代码，其类名可自定义。

第二个类称之为**爬虫类**，即专门编写用于生成新请求的代码，其类名必须为`Spider`。

爬虫从`Spider.start`方法开始，`Spider`的每个方法均返回一个或多个`Request`实例，每一个`Request`最终会被包装成一个`Task`实例。

`Request`的一个重要参数是`callback`，其用于确定该请求的回调方法是什么。`callback`是一个或多个字典（字典或列表）。对于每个字典有`parser`与`fetcher`(可选)两个键值。在该请求成功之后，解析器`Parser`即运行`parser`键值指定的回调方法，解析出用户所需要的数据之后，再调用`fetcher`键值制定的回调方法，重新生成新的请求。如该例子的`start`方法，其`callback`为`{'parser': self.parser_index_page, 'fetcher': self.detail_page}`，即该请求成功之后，调用`MyParser.parser_index_page`方法进行解析，解析完成后调用`Spider_detail_page`生成新的请求。

# 爬虫类与解析类

继承于`catty.spider.BaseSpider`的类，我们称其为**爬虫类**

继承于`catty.parser.BaseParser`的类，我们称其为**解析类**

## 爬虫类

爬虫类每一个方法都接收解析出来的数据，并生成新的请求。`Parser`解析得到的数据在task\['parser'\]\['item'\]里面，通过参数`task`传递。方法调用其父类`BaseSpdier.Request`方法，生成一个或多个请求并返回。

爬虫类有一些特定的属性

#### 类属性

- name(str)：爬虫的名字，必须与无".py"后缀的文件名一致
- speed(int)：可选，爬虫的初始速度
- seeds(list)：可选，BloomFilter哈希函数的种子，有多少个种子即代表有多少个哈希函数
- blocknum(int)：可选，BloomFilter的bitmap数量

## 解析类

解析类每一个方法都接受请求成功的`Response`，并进行解析和返回结果。解析的类方法可以是使用`async`与`await`的异步方法，也可以是普通的同步的方法。接收参数`response`，`task`（可选），`loop`（异步是必选）。

# API 参考

## BaseSpider
### request(self, **kwargs)

每个爬虫脚本的**爬虫类**需继承`BaseSpider`并实现`start`方法用于作为爬虫的调用入口，并调用`BaseSpider.request`生成新请求。

**Params**

- method(str)：HTTP请求的方法，默认为"GET"

- url(str)：请求的URL

- callback(list)：回调函数，用于请求成功之后的回调

- meta(dict)：用于特定的功能或将数据通过其传之下一个请求
  - retry(int)：最大重试次数。默认为0，不重试
  - retry_wait(int)：重试的时间间隔，0为立刻重试。默认为3
  - dupe_filter(int)：是否使用去重

- params(dict/bytes)：可选，追加到URL后面的的URL参数

  以下两个请求是相同的

  ```python
  def start(self):
    return self.request(
        url='http://applehater.cn/',
        params={'a':123}
    )
  def start(self):
    return self.request(
        url='http://applehater.cn?a=123',
    )
  ```


- data(dict/bytes)：可选，Request里body的数据

- headers(dict)：可选，请求的头部

- allow_redirects(bool)：可选，允许重定向

- timeout(int)：可选，超时时间，单位秒，默认300

- priority(int)：可选，优先级，数值越大，优先级越高，默认1


## BaseParser

### retry_current_task()

抛出一个`catty.excption.Retry_current_task`异常，重试当前请求。

## Request 与 Response

### Request

Request对象通过调用`catty.spider.BaseSpider.request`方法生成，其主要属性参数详见`BaseSpider.request`

### Response

- status(int)：HTTP状态码
- method(str)：HTTP请求方法
- cookies(aiohttp.SimpleCookies)：HTTP响应头部的cookies（`Set-Cookie` HTTP头部）
- headers(list)：HTTP响应头部，一连串(key, value)元组对
- charset(str)：HTTP响应头部`Content-Type`的值
- body(bytes)：响应的body
- use_time(float)：完成请求所需的时间

# 架构介绍

![Catty 架构](/catty_architecture.png)

### 组件

### 调度器（Scheduler）

调度器是爬虫系统的核心，其通过消息队列与下载器以及解析器进行通信，接收解析器解析出来的超链接，制作任务（Task）。并根据优先级，速度控制等信息为基准将制作的任务放入消息队列中让下载器进行页面的下载。

### 下载器（Downloader）

下载器通过消息队列，获取调度器发送过来的任务，对网页进行请求与下载。并将结果放入消息队列中传给解析器。下载器底层使用如今Python最热门的异步HTTP库Aiohttp。

### 解析器（Parser）

解析器是解析下载回来的HTML文件的部分，其不拘泥于某一种解析方法。使用何种解析方法是用户在爬虫程序中定义的（用户可使用正则表达式或建立HTML DOM树）。解析器对下载回来的HTML页面进行解析后，得到用户需要的信息，例如超链接或者特定位置的文本等。最后将信息附在任务（task）中。解析器亦作保存结果的处理，其支持异步的方法，可以避免写文件以及写数据库时阻塞整个主线程。

## 任务（Task）

任务是一个请求的上下文，在Python里面以字典(dict)的形式存在。任务在调度器里生成，通过下载器与解析器，最后到达调度器，重新生成新的任务。

一个Task的完整结构如下：

```
'task': {
    'tid': str,                         md5(request.url + request.data)
    'spider_name': str,                 Spider name
    'priority': int,                    Priority of task
    'retried': int,                     Retried count
    'meta':dict                         A dict to some config or something to save
    {
        'retry': int                    The count of retry.Default: 0
        'retry_wait': int               Default: 3
        'dupe_filter': bool             Default: False
    },
    
    'request': Request_obj
    {
        'method':str                    HTTP method
        'url':str                       URL
        'params':dict/str               string must be encoded(optional)
        'data':dict/bytes               to send in the body of the request(optional)
        'headers':dict                  HTTP Headers(optional)
        'allow_redirects':bool			Allow redirects
        'timeout':int                   a timeout for IO operations, 5min by default(option).
    }
    
    'downloader': {
    },
    
    'scheduler': {
    },
    
    'parser': {
        'item': dict,          			The dict return from parser func
    },
    
    'response': Response_obj,
    {
        'status':int                    HTTP status code
        'method':str                    HTTP method
        'cookies':SimpleCookie          HTTP cookies of response (Set-Cookie HTTP header)
        'headers':list                  HTTP headers of response,a sequence of (key, value) pairs.
        'content_type':str              Content-Type header
        'charset':str                   The value is parsed from the Content-Type HTTP header.
        'body':bytes                    response’s body as bytes.
        'use_time':float                the time cost in request
    }
    
    'callback': list,
}
```

## 爬虫（Spider）

一个爬虫，又称为爬虫脚本。一个爬虫脚本包含了解析类与爬虫类。其类定义了如何爬取某个(或某些)网站。包括了爬取的动作(例如:是否跟进链接)以及如何从网页的内容中提取结构化数据。

## 消息队列（Message Queue）

消息队列是一种进程间通信或同一进程的不同线程间的通信方式。[5]该爬虫系统中使用消息中间件，用于该爬虫系统三大组件之间的解耦。消息队列的实现我们选用Redis。Redis是一个使用ANSI C编写的开源、支持网络、基于内存、可选持久性的键值对存储数据库。虽然Redis是一个NoSQL数据库，但我们完全可以将其当做一个轻量级的队列服务来使用。对于我们这个爬虫系统来说，我们的消息队列不需要很大的吞吐量（每秒几千个请求即可），所以我们选用Redis来实现一个轻量级的消息队列。

在我们的爬虫系统中，一共有四个消息队列，分别是：

- 调度器-下载器队列（scheduler-downloader-queue）
- 下载器-解析器队列（downloader-parser-queue）
- 解析器-调度器队列（parser-scheduler-queue)
- 请求队列（request-queue）

## 数据流（Data flow）

1. 每个爬虫脚本需实现一个`start`方法，当在WebUI里面**start**之后，调度器即执行该爬虫脚本的`start`方法，生成一个任务（我们称其为种子任务）。将其放入每个爬虫的请求队列中。
2. 调度器的中间件Selector将根据优先级，速度控制等信息为基准从每个爬虫的请求队列中拿出任务放入调度器-下载器队列中。
3. 下载器从调度器-下载器队列中取出任务，进行页面的下载并将HTML页面等结果追加在任务里，最后将任务放入下载器-解析器队列中。
4. 解析器从下载器-解析器队列中取出任务，进行页面解析或者数据的保存，最后将解析得到的结果追加到任务里，放入解析器-调度器队列中。
5. 调度器从解析器-结果流水线队列中取出任务，根据解析结果以及回调函数生成新的任务。重复2-5步骤，直至没有多余的任务为止。

## 关于去重（DupeFilter）

Catty使用Redis+BloomFilter实现URL的去重。Catty的BloomFilter使用了Redis的Bitmap数据结构，默认一个Block是64MB，用户可以预估爬虫的规模来设定Block数量。经过计算，在错误率为0.1%的时候，1亿个URL去重需要125MB。使用MD5加盐值(SALT)作为哈希函数对URL进行散列，用户可以给每个爬虫设定多个盐值从而减低错误率。

## WebUI

WebUI是Catty的网页管理工具，其可以：

- 停止，启动爬虫
- 上传新爬虫
- 修改爬虫速度
- 查看爬虫结果数量


![WebUI](/webui.png)

## 事件驱动网络(Event-driven networking)

Catty基于事件驱动网络框架 [Aiohttp](http://aiohttp.readthedocs.org) 编写。因此，Catty基于并发性考虑由非阻塞(即异步)的实现。

# 关于性能

通过如下脚本作性能测试

```python
# 服务器端
from sanic import Sanic
from sanic.response import json

app = Sanic()

@app.route("/")
async def test(request):
    return json({"hello": "world"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
```

```python
# 爬虫
from catty.parser import BaseParser
from catty.spider import BaseSpider

import time
import random


class MyParser(BaseParser):
    def parser(self, response):
        return {'a': time.time() + random.randint(0, 9999999)}


class Spider(BaseSpider, MyParser):
    name = 'benchmark'

    def start(self):
        return [self.request(
            url='http://127.0.0.1:8000?a={}'.format(i),
            callback={'parser': self.parser, 'fetcher': self.get_next}
        ) for i in range(10000)]
    
    def get_next(self, task):
        return self.request(
            url='http://127.0.0.1:8000?a={}'.format(task['parser']['item']['a']),
            callback={'parser': self.parser, 'fetcher': self.get_next}
        )
```

该测试在作者的电脑下为18513页/分钟。

使用如今Python社区最火的Scrapy的bench测试，测试得Scrapy在作者的电脑下为5400页/分钟。

当然，Scrapy是单进程模式运行的，这会限制其速度。Catty的调度器，下载器，解析器分别都为不同的进程，可以充分利用多核CPU。

作者仅在此做一个非常简单的爬虫。 若您所编写爬虫做更多处理，会减慢爬取的速度。 减慢的程度取决于爬虫所做的处理。