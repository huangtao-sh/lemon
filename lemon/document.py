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
    
    @classproperty
    def _acollection(cls):
        if Document.__adb is None:
            from motor.motor_asyncio import AsyncIOMotorClient
            client=AsyncIOMotorClient(**config())
            Document.__adb=client.get_default_database()
        return Document.__adb[convert_cls_name(cls.__name__)]

    @classproperty
    def _collection(cls):
        if Document.__db is None:
            client=MongoClient(**config())
            Document.__db=client.get_default_database()
        return Document.__db[convert_cls_name(cls.__name__)]

    def __str__(self):
        if hasattr(self,'_textfmt'):
            return self._textfmt.format(self=self)
        else:
            return super().__str__()

    def __getattr__(self,attr):
        return self[attr]

    @classmethod
    def drop(cls):
        return cls._collection.drop()

    drop_collection=drop
    
    @classmethod
    def insert(cls,*docs):
        func=cls._collection.insert_one if len(docs)==1 else \
          cls._collection.insert_many
        return func(*docs)

    @classmethod
    def find(cls,q,**kw):
        kw.update(q.to_query())
        return cls._collection.find(kw)

    @classproperty
    def objects(cls):
        return BaseQuery(cls)
        
    @classmethod
    def aggregate(cls,*args,**kw):
        return Aggregation(cls,*args,**kw)
    
