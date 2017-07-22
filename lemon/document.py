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

    @classmethod
    def insert(cls,*args,**kw):
        return cls._collection.insert(*args,**kw)

    @classmethod
    def find(cls,q,**kw):
        kw.update(q.to_query())
        return cls._collection.find(kw)

    @classproperty
    def objects(cls):
        return BaseQuery(cls)
        
    def __str__(self):
        return self._textfmt.format(self=self)

    def __getattr__(self,attr):
        return self[attr]

    @classmethod
    def aggregate(cls,*args,**kw):
        return Aggregation(cls,*args,**kw)
    
class Abc(Document):
    _textfmt='a={self.a}\tb={self.b}'

    
async def main():
    #await batch(lambda x:Abc.insert({'a':x,'b':x+10}),range(20))
    a=Abc.aggregate()
    a.project(-P.id,P.a,P.b)
    a.group(P.a,P.b.sum(1))
    a.match(P.id>5)
    a.sort(P.id)
    a.skip(10)
    a.limit(5)
    async for i in a:
        print(i)
    
start(main())

        
            
            
        
