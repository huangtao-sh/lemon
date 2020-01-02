# 项目：协程版Mogodb
# 模块：数据库模型
# 作者：黄涛
# License:GPL
# Email:huangtao.sh@icloud.com
# 创建：2017-07-22 09:52
# 修订：2018-09-11 新增 Descriptor 类
# 修改：2018-10-15 22:03 增加 show, profile 等功能
# 修订：2019-12-21 16:10 进行修订

from orange import convert_cls_name, cachedproperty, wlen, tprint, split, Data, classproperty
from .query import BaseQuery, Aggregation, AsyncioQuery, P
from .config import config, get_client
from .loadfile import ImportFile, FileImported, enlist
from .bulk import BulkWrite, BulkResult
from bson import ObjectId

MAX_BULK_SIZE = 100000


class DocumentMeta(type):
    @property
    def objects(cls) -> BaseQuery:
        return BaseQuery(cls)

    @property
    def abjects(cls) -> AsyncioQuery:
        return AsyncioQuery(cls)

    def drop(cls):
        return cls._collection.drop()

    drop_collection = drop

    def insert_one(cls, *args, **kw):
        return cls._collection.insert_one(*args, **kw)

    def insert_many(cls, *args, **kw):
        return cls._collection.insert_many(*args, **kw)

    def ansert_one(cls, *args, **kw):
        return cls._acollection.insert_one(*args, **kw)

    def ansert_many(cls, *args, **kw):
        return cls._acollection.insert_many(*args, **kw)


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
    _projects = ()
    _textfmt = ''  # 文本格式
    _htmlfmt = ''  # 超文本格式
    _profile = {}  # profile 属性时使用

    @classmethod
    def get_collection(cls, sync=False):
        client = get_client(sync)
        database = client.get_default_database()
        return database.get_collection(convert_cls_name(cls.__name__))

    @classmethod
    def create_indexes(cls, *indexes):
        collection = cls.get_collection()
        for index in indexes:
            collection.create_index(index)

    @classmethod
    async def load_files(cls, *files, clear=False, dup_check=True, **kw):
        for fn in files:
            await cls.amport_file(fn, drop=clear, dupcheck=dup_check, **kw)

    @classmethod
    def aggregate(cls, pipeline=None, **kw):
        return Aggregation(cls, pipeline, **kw)

    @classmethod
    def find(cls, *args, **kw) -> BaseQuery:
        return cls.objects.filter(*args, **kw)

    @classmethod
    def _bulk_write(cls,
                    requests,
                    ordered=True,
                    bypass_document_validation=False,
                    session=None):
        collection = cls.get_collection()
        result = BulkResult()
        for reqs in split(requests, MAX_BULK_SIZE):
            result += collection.bulk_write(
                reqs,
                ordered=True,
                bypass_document_validation=bypass_document_validation,
                session=session)
        return result

    @classmethod
    async def sync_bulk_write(cls,
                              requests,
                              ordered=True,
                              bypass_document_validation=False,
                              session=None):
        collection = cls.get_collection(sync=True)
        result = BulkResult()
        for reqs in split(requests, MAX_BULK_SIZE):
            result += await collection.bulk_write(
                reqs,
                ordered=True,
                bypass_document_validation=bypass_document_validation,
                session=session)
        return result

    @classmethod
    def bulk_write(cls,
                   data: Data = None,
                   requests=None,
                   mapper: dict = None,
                   fields=None,
                   keys=None,
                   upsert=True,
                   ordered=True,
                   drop=True,
                   method='insert'):
        if method == 'insert' and drop:
            cls.objects.delete()
        return BulkWrite(cls,
                         requests=requests,
                         data=data,
                         mapper=mapper,
                         fields=fields,
                         keys=keys,
                         upsert=upsert,
                         drop=drop,
                         method=method).execute(ordered=ordered)

    @property
    def id(self):
        return self.get('_id')

    @id.setter
    def id(self, value):
        self['_id'] = value

    def _save(self) -> bool:
        if self._modified:
            if self.id is None:
                self._id = ObjectId()
            self._modified = False
            return True

    def save(self):
        if self._save():
            self.get_collection().find_one_and_replace({'_id': self._id},
                                                       self,
                                                       upsert=True)
            return self

    async def asave(self):
        if self._save():
            await self.get_collection(True).find_one_and_replace(
                {'_id': self._id}, self, upsert=True)
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
        return self.get_collection(sync=True)

    @cachedproperty
    def _collection(self):
        return self.get_collection()

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
