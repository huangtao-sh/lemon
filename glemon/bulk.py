# 项目：   MongoDB 数据库
# 模块：   数据处理模块
# 作者：   黄涛
# License: GPL
# Email:   huangtao.sh@icloud.com
# 创建：2019-12-15 15:56

from orange import Data, Path, split
from pymongo import InsertOne, ReplaceOne, UpdateOne, UpdateMany
from .loadfile import enlist

MAX_SIZE = 100000


class BulkResult(object):
    def __init__(self):
        self.result = {}
        for key in ('writeErrors', 'upserted'):
            self.result[key] = []
        for key in ('nInserted', 'nRemoved', 'nMatched', 'nModified',
                    'nUpserted'):
            self.result[key] = 0

    def __call__(self, other):
        other = other.bulk_api_result
        for key in ('writeErrors', 'upserted'):
            self.result[key].extend(other[key])
        for key in ('nInserted', 'nRemoved', 'nMatched', 'nModified',
                    'nUpserted'):
            self.result[key] += other[key]

    def __str__(self):
        return '\t'.join(f'{key}:{self.result.get(key):,d}'
                         for key in ('nInserted', 'nMatched', 'nModified',
                                     'nRemoved'))

    @property
    def matched_count(self):
        return self.result.get('nMatched')

    @property
    def inserted_count(self):
        return self.result.get('nInserted')

    @property
    def modified_count(self):
        return self.result.get('nModified')

    @property
    def Upserted_count(self):
        return self.result.get('nUpserted')

    @property
    def Removed_count(self):
        return self.result.get('nRemoved')


METHOD = {
    'insert': InsertOne,
    'InsertOne': InsertOne,
    'update': UpdateOne,
    'UpdateOne': UpdateOne,
    'replace': ReplaceOne,
    'ReplaceOne': ReplaceOne,
    'UpdateMany': UpdateMany
}


class BulkWrite(object):
    def __init__(self,
                 document: 'Document',
                 requests=None,
                 data: Data = None,
                 mapper: dict = None,
                 fields=None,
                 keys=None,
                 upsert=True,
                 drop=True,
                 method='insert'):
        self.document = document
        self.requests = requests
        if data:
            if not isinstance(data, Data):
                data = Data(data)
            self.method = METHOD.get(method, None)
            if mapper:
                self.fields = mapper.keys()
                data.columns(mapper.values())
            else:
                self.fields = enlist(fields or document._projects)
            
            self._data = data
            self.keys = enlist(keys or '_id')
            self.method = METHOD.get(method, None)
            self.upsert = upsert

    def __iter__(self):
        if self.requests:
            yield from self.requests
        else:
            method = self.method
            if method == InsertOne:
                fields = self.fields
                yield from self._data.converter(
                    lambda row: InsertOne(dict(zip(fields, row))))
            elif method in (UpdateOne, UpdateMany):
                key_indexes, value_indexes, fields = [], [], []
                keys = self.keys

                for i, f in enumerate(self.fields):
                    if f in keys:
                        key_indexes.append(i)
                    else:
                        fields.append(f)
                        value_indexes.append(i)
                yield from self._data.converter(lambda row: method(
                    dict(zip(keys, [row[i] for i in key_indexes])),
                    {'$set': dict(zip(fields, [row[i] for i in value_indexes]))},
                    upsert=self.upsert))
            elif method == ReplaceOne:
                key_indexes, fields = [], self.fields
                keys = self.keys

                for i, f in enumerate(self.fields):
                    if f in keys:
                        key_indexes.append(i)
                yield from self._data.converter(lambda row: ReplaceOne(
                    dict(zip(keys, [row[i] for i in key_indexes])),
                    dict(zip(fields, row)),
                    upsert=self.upsert))
            else:
                raise Exception('method 参数错误')

    def execute(self,
                ordered=True,
                bypass_document_validation=False,
                session=None):
        collection = self.document._collection
        result = BulkResult()
        for data in split(self, MAX_SIZE):
            result(
                collection.bulk_write(list(data), ordered,
                                      bypass_document_validation, session))
        return result

    async def sync_execute(self,
                           ordered=True,
                           bypass_document_validation=False,
                           session=None):
        collection = self.document._acollection
        result = BulkResult()
        for data in split(self, MAX_SIZE):
            result(await
                   collection.bulk_write(list(data), ordered,
                                         bypass_document_validation, session))
        return result
