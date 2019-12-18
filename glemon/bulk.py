# 项目：   MongoDB 数据库
# 模块：   数据处理模块
# 作者：   黄涛
# License: GPL
# Email:   huangtao.sh@icloud.com
# 创建：2019-12-15 15:56

from orange import Data, Path
from .document import enlist, Document, InsertOne, ReplaceOne, UpdateOne, split
from asyncio import wait

MAX_SIZE = 100000


class BulkResult(object):
    def __init__(self, result={}):
        self.result = result
        for key in ('writeErrors', 'upserted'):
            self.result.setdefault(key, [])
        for key in ('nInserted', 'nRemoved', 'nMatched', 'nModified',
                    'nUpserted'):
            self.result.setdefault(key, 0)

    def __call__(self, result):
        for key in ('writeErrors', 'upserted'):
            self.result[key].extend(result[key])
        for key in ('nInserted', 'nRemoved', 'nMatched', 'nModified',
                    'nUpserted'):
            self.result[key] += result[key]

    def __str__(self):
        return (f'Inserted : {self.result["nInserted"]:9,d}\n'
                f'Matched  : {self.result["nMatched"]:9,d}\n'
                f'Modified : {self.result["nModified"]:9,d}\n'
                f'Upserted : {self.result["nUpserted"]:9,d}\n'
                f'Removed  : {self.result["nRemoved"]:9,d}\n'
                f'Errors   : {self.result["writeErrors"]}')


class BulkWrite(object):
    def __init__(self,
                 document: Document,
                 data: Data,
                 fields=None,
                 keys=None,
                 method='insert'):
        self.document = document
        if not isinstance(data, Data):
            data = Data(data)
        self._data = data
        self.fields = fields or enlist(document._projects)
        self.keys = enlist(keys or '_id')
        if method not in ('insert', 'update', 'replace'):
            raise Exception('Error: method must be insert,update or replace')
        self.method = method

    def __iter__(self):
        if self.method == 'insert':
            fields = self.fields
            yield from self._data.converter(lambda row: InsertOne(
                dict(zip(fields, row))))
        elif self.method == 'update':
            key_indexes, value_indexes, fields = [], [], []
            keys = self.keys

            for i, f in enumerate(self.fields):
                if f in keys:
                    key_indexes.append(i)
                else:
                    fields.append(f)
                    value_indexes.append(i)
            yield from self._data.converter(lambda row: UpdateOne(
                dict(zip(keys, [row[i] for i in key_indexes])),
                {'$set': dict(zip(fields, [row[i] for i in value_indexes]))},
                upsert=True))
        else:
            key_indexes, fields = [], self.fields
            keys = self.keys

            for i, f in enumerate(self.fields):
                if f in keys:
                    key_indexes.append(i)
            yield from self._data.converter(lambda row: ReplaceOne(
                dict(zip(keys, [row[i] for i in key_indexes])),
                dict(zip(fields, row)),
                upsert=True))

    def execute(self,
                ordered=True,
                bypass_document_validation=False,
                session=None):
        collection = self.document._collection
        result = BulkResult()
        for data in split(self, MAX_SIZE):
            result(
                collection.bulk_write(list(data), ordered,
                                      bypass_document_validation,
                                      session).bulk_api_result)
        return result

    async def sync_execute(self,
                           ordered=True,
                           bypass_document_validation=False,
                           session=None):
        collection = self.document._acollection
        result = []
        for data in split(self, MAX_SIZE):
            result.append(
                collection.bulk_write(list(data), ordered,
                                      bypass_document_validation, session))
        return result
