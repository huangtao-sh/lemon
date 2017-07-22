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
    
class Abc(Document):
    _textfmt='a={self.a}\tb={self.b}'

    
async def main():
    abc=Abc.objects(P.a>10)(P.a<=200)
    async for i in abc:
        print(i)
    print('-'*20)
    await Abc.objects(P.a>=12).update(P.b.inc(1))
    async for i in abc:
        print(i)
        
start(main())

        
            
            
        
