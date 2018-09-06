# 项目：协程版Mogodb
# 模块：数据库模型
# 作者：黄涛
# License:GPL
# Email:huangtao.sh@icloud.com
# 创建：2017-07-22 09:52

from pymongo import MongoClient
from orange import convert_cls_name, cachedproperty
from .query import BaseQuery, Aggregation, AsyncioQuery, P
from .config import config
from .loadfile import ImportFile, FileImported


class DocumentMeta(type):
    _db_cache = {}
    _collection_cache = {}

    @property
    def objects(cls):
        return BaseQuery(cls)

    @property
    def abjects(cls):
        return AsyncioQuery(cls)

    def drop(cls):
        return cls._collection.drop()

    drop_collection = drop

    def aggregate(cls, *args, **kw):
        return Aggregation(cls, *args, **kw)

    def insert_one(cls, *args, **kw):
        return cls._collection.insert_one(*args, **kw)

    def insert_many(cls, *args, **kw):
        return cls._collection.insert_many(*args, **kw)

    def ansert_one(cls, *args, **kw):
        return cls._acollection.insert_one(*args, **kw)

    def ansert_many(cls, *args, **kw):
        return cls._acollection.insert_many(*args, **kw)


class Document(dict, ImportFile, metaclass=DocumentMeta):
    __db = None
    __adb = None
    _projects = ()
    _textfmt = ''    # 文本格式
    _htmlfmt = ''    # 超文本格式

    @classmethod
    async def load_files(cls, *files, clear=False, dup_check=True, **kw):
        for fn in files:
            await cls.amport_file(fn, drop=clear, dupcheck=dup_check, **kw)

    @property
    def id(self):
        return self.get('_id')

    @id.setter
    def id(self, value):
        self['_id'] = value

    def save(self):
        if self._modified:
            if self.id:
                d = self.copy()
                d.pop('_id')
                self.__class__.objects(P.id == self.id).upsert_one(**d)
            else:
                self._collection.insert_one(self)
            self._modified = False
        return self

    async def asave(self):
        if self._modified:
            if self.id:
                d = self.copy()
                d.pop('_id')
                await self.__class__.abjects(P.id == self.id).upsert_one(**d)
            else:
                await self._collection.insert_one(self)
            self._modified = False
        return self

    def __setitem__(self, *args, **kw):
        self._modified = True
        return super().__setitem__(*args, **kw)

    @property
    def _text(self):
        # 返回本实例的文本格式
        return self._textfmt.format(self=self)

    def __str__(self):
        # 返回本实例的文本格式
        return self._text if self._textfmt else super().__str__()

    @property
    def _html(self):
        # 返回本实例的超文本格式
        return self._htmlfmt.format(self=self)

    def __init__(self, *args, id=None, from_query=False, **kw):
        self._modified = not from_query
        if id:
            kw['_id'] = id
        super().__init__(*args, **kw)

    @cachedproperty
    def _acollection(self):
        if Document.__adb is None:
            from motor.motor_asyncio import AsyncIOMotorClient
            client = AsyncIOMotorClient(**config())
            Document.__adb = client.get_database()
        return Document.__adb[convert_cls_name(self.__name__)]

    @cachedproperty
    def _collection(self):
        if Document.__db is None:
            client = MongoClient(**config())
            Document.__db = client.get_database()
        return Document.__db[convert_cls_name(self.__name__)]

    def values(self, *fields):
        return tuple((self.get(p, None) for p in fields))

    def __getattr__(self, attr):
        return self.get(attr)

    def __setattr__(self, name, value):
        if name in self._projects:
            self[name] = value
        else:
            super().__setattr__(name, value)

    def update(self, *args, **kw):
        self._modified = True
        self.update(*args, **kw)
