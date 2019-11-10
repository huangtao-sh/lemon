# 项目：协程版Mogodb
# 模块：数据库模型
# 作者：黄涛
# License:GPL
# Email:huangtao.sh@icloud.com
# 创建：2017-07-22 09:52
# 修订：2018-09-11 新增 Descriptor 类
# 修改：2018-10-15 22:03 增加 show, profile 等功能

from pymongo import MongoClient, InsertOne, UpdateOne, ReplaceOne
from orange import convert_cls_name, cachedproperty, wlen, tprint, split
from .query import BaseQuery, Aggregation, AsyncioQuery, P
from .config import config
from .loadfile import ImportFile, FileImported, enlist


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

    def bulk_write(cls,
                   data,
                   mapper=None,
                   fields=None,
                   keys=None,
                   method='insert',
                   drop=True,
                   ordered=True):
        if not mapper:
            fields = enlist(fields or cls._projects)
            mapper = {k: i for i, k in enumerate(fields) if k}
        if method == 'insert':
            fields = mapper.items()

            def add(row):
                return InsertOne(dict((x, row[y]) for x, y in fields))
        else:
            keys = enlist(keys or '_id')
            if method == 'replace':
                keymapper = tuple((k, mapper[k]) for k in keys)

                def _add(x, y):
                    return ReplaceOne(x, y, upsert=True)
            else:
                keymapper = tuple((k, mapper.pop(k)) for k in keys)

                def _add(x, y):
                    return UpdateOne(x, {'$set': y}, upsert=True)

            fields = mapper.items()

            def add(row):
                return _add(dict((x, row[y]) for x, y in keymapper),
                            dict((x, row[y]) for x, y in fields))

        if data:
            if method == 'insert' and drop:
                cls._collection.drop()
            return cls._collection.bulk_write(list(map(add, data)),
                                              ordered=ordered)


class Descriptor(dict):
    format_spec = '{key}-{value}'

    def __init__(self, field, *args, format_spec=None, **kw):
        self.field = field
        if format_spec:
            self.format_spec = format_spec
        self.update(*args, **kw)

    def __get__(self, obj, type):
        if obj:
            key = obj.get(self.field)
            value = self.get(key)
            return self.format(key, value)
        else:
            return self

    def format(self, key, value):
        return self.format_spec.format(key=key, value=value)


class Document(dict, ImportFile, metaclass=DocumentMeta):
    __db = None
    __adb = None
    _projects = ()
    _textfmt = ''  # 文本格式
    _htmlfmt = ''  # 超文本格式
    _profile = {}  # profile 属性时使用

    @classmethod
    def load(cls,
             data,
             mapper=None,
             fields=None,
             keys=None,
             method='insert',
             drop=True,
             ordered=True,
             batch=100000):
        if method == 'insert' and drop:
            cls.drop()
        for data in split(data, batch):
            cls.bulk_write(data,
                           mapper,
                           fields=fields,
                           keys=keys,
                           drop=False,
                           ordered=ordered)

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
                type(self).objects.filter(_id=self.id).upsert_one(**d)
            else:
                self._collection.insert_one(self)
            self._modified = False
        return self

    async def asave(self):
        if self._modified:
            if self.id:
                d = self.copy()
                d.pop('_id')
                await type(self).abjects.filter(_id=self.id).upsert_one(**d)
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
        projects = self._projects
        return tuple(
            self.get(p, None) if p in projects else getattr(self, p)
            for p in fields)

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

    def show(self, profile=None, format_spec=None, sep='    '):
        profile = profile or self._profile
        if not format_spec:
            length = max(map(wlen, profile.keys()))
            format_spec = {0: str(length)}
        data = [(name, getattr(self, field))
                for name, field in profile.items()]
        tprint(data, format_spec=format_spec, sep=sep)

    @classmethod
    def find(cls, *args, **kw):
        return cls.objects.filter(*args, **kw)
