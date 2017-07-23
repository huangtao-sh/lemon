# 项目：协程版Mogodb
# 模块：数据库模型
# 作者：黄涛
# License:GPL
# Email:huangtao.sh@icloud.com
# 创建：2017-07-22 09:52

from motor.motor_asyncio import *
from .query import *
from orange.coroutine import *

class Document(dict):
    __config={}
    __db=None
    
    @classproperty
    def _config(cls):
        return Document.__config
    
    @classproperty
    def _collection(cls):
        if Document.__db is None:
            client=AsyncIOMotorClient(**cls._config)
            Document.__db=client.test
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
    
