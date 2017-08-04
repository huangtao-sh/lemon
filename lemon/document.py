# 项目：协程版Mogodb
# 模块：数据库模型
# 作者：黄涛
# License:GPL
# Email:huangtao.sh@icloud.com
# 创建：2017-07-22 09:52

from pymongo import *
from .query import *
from .config import *

class Document(dict):
    __db=None
    __adb=None
    _projects=()

    def __init__(self,*args,from_query=False,**kw):
        self._modified=not from_query
        super().__init__(*args,**kw)
        
    @cachedproperty
    def _acollection(cls):
        if Document.__adb is None:
            from motor.motor_asyncio import AsyncIOMotorClient
            client=AsyncIOMotorClient(**config())
            Document.__adb=client.get_default_database()
        return Document.__adb[convert_cls_name(cls.__name__)]
    
    @cachedproperty
    def _collection(cls):
        if Document.__db is None:
            client=MongoClient(**config())
            Document.__db=client.get_default_database()
        return Document.__db[convert_cls_name(cls.__name__)]

    def values(self,*fields):
        return tuple((getattr(self,p) for p in fields))

    def __str__(self):
        if hasattr(self,'_textfmt'):
            return self._textfmt.format(self=self)
        else:
            return super().__str__()

    def __getattr__(self,attr):
        return self[attr] if attr in self._projects else \
            super().__getattr__(attr)


    def __setattr__(self,name,value):
        if name in self._projects:
            self[name]=value
        else:
            super().__setattr__(name,value)

    @classmethod
    def drop(cls):
        return cls._collection.drop()

    drop_collection=drop
    
    @classproperty
    def objects(cls):
        return BaseQuery(cls)
    
    @classproperty
    def abjects(cls):
        return AsyncioQuery(cls)
        
    @classmethod
    def aggregate(cls,*args,**kw):
        return Aggregation(cls,*args,**kw)
    
    @classmethod
    def insert_one(cls,*args,**kw):
        return cls._collection.insert_one(*args,**kw)

    @classmethod
    def insert_many(cls,*args,**kw):
        return cls._collection.insert_many(*args,**kw)

    @classmethod
    def ansert_one(cls,*args,**kw):
        return cls._acollection.insert_one(*args,**kw)

    @classmethod
    def ansert_many(cls,*args,**kw):
        return cls._acollection.insert_many(*args,**kw)
