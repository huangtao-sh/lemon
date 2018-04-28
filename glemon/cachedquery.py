'''
项目： 数据库模型
模块： 查询缓存
创建： 2017-12-9

'''

from glemon import Document
import json
from importlib import import_module


class CachedQuery(Document):
    _projects = '_id', "module", "name", "query", "extra"
    '''
    缓存查询，主要用于网页分页查询使用，
    第一次查询时，可以进行缓存
    后续使用分页来进行调取
    '''

    @classmethod
    def dumps(cls, query, **kw):
        return cls(module=query.document.__module__,
                   name=query.document.__name__,
                   query=json.dumps(query.dumps()),
                   extra=kw).save()

    @classmethod
    def loads(cls, _id, kw=None):
        obj = cls.objects.get(_id)
        if obj:
            module = import_module(obj.module)
            document = getattr(module, obj.name)
            objects = document.objects
            objects.__dict__.update(json.loads(obj.query))
            if kw:
                kw.update(obj.extra)
            else:
                objects.extra = obj.extra
            return objects


def load_query(_id):
    return CachedQuery.loads(_id)
