# 项目：数据库模型
# 模块：客户端
# 作者：黄涛
# License:GPL
# Email:huangtao.sh@icloud.com
# 创建：2017-07-31 20:35

from orange import is_installed
from orange.config import Config
from pymongo import MongoClient
import sys
__config = None
__db = None
__adb = None


def get_client():
    global __db
    if not __db:
        __db = MongoClient(**config())
    return __db


def config(is_dev=None, **kw):
    global __config
    if not __config:
        if is_dev is None:
            is_dev = not ('wsgi' in sys.argv[0] or is_installed(
                sys.argv[0])) or 'test' in sys.argv[0]
        _config = Config(project='mongo', is_dev=is_dev)
        _config.load_config()
        config = _config.get('database') or {}
        config.setdefault('host', 'mongodb://localhost/mongo')
        config.setdefault('tz_aware', True)
        config.setdefault('connect', False)
        config.setdefault('retryWrites', 'false')
        if _config.is_dev:
            config['host'] = 'mongodb://localhost/test'
        __config = config
    if kw:
        __config.update(kw)
    return __config
