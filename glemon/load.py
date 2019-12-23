# 项目：   MongoDB 数据库
# 模块：   文件导入模块
# 作者：   黄涛
# License: GPL
# Email:   huangtao.sh@icloud.com
# 创建：2019-12-21 17:18

from glemon.document import Document
from orange import Path, Data, limit
from .bulk import BulkWrite
from .loadcheck import LoadFile


class LoadDocument(Document):
    load_options = {
    }  # file :reader: callable,encoding,errors,columns,converter,delimter,sep

    @classmethod
    def read_file(cls, path, reader=None, **kw):
        if callable(reader):
            return reader(path)
        else:
            if path.lsuffix in ('.csv', '.del'):
                return Path(path).iter_csv(**kw)
            else:
                return Path(path).read_data(**kw)

    @classmethod
    def _bulk(cls,
              data: Data,
              header: dict = None,
              mapper: dict = None,
              fields=None,
              keys=None,
              upsert=True,
              ordered=True,
              drop=True,
              method='insert'):
        if header:
            mapper, converter = {}, {}
            _header = set(header.keys())
            for d in data:
                if not (_header - set(d)):
                    break
            else:
                return
            for k, v in header.items():
                idx = d.index(k)
                if isinstance(v, (tuple, list)) and len(v) == 2:
                    field, conv = v
                    mapper[field] = idx
                    converter[idx] = conv
                elif isinstance(v, str):
                    mapper[v] = idx
            if converter:
                data.converter(converter)
        if drop and method in ('insert', 'insertOne'):
            cls.drop()
        return BulkWrite(cls, data, mapper, fields, keys, upsert, drop, method)

    @classmethod
    def load_file(cls, file, options=None, dry=False):
        options = options or cls.load_options
        dupcheck = options.pop('dupcheck')
        if dupcheck:
            checker = LoadFile.dupcheck(file, 'loadfile')
        data = cls.read_file(file, **options.pop('file', {}))
        blk = cls._bulk(data, **options)
        if blk:
            if dry:
                for x in blk:
                    print(x)
            else:
                result = blk.execute()
                if dupcheck:
                    checker.save()
                return result

    @classmethod
    async def sync_load_file(cls, file, options=None):
        options = options or cls.load_options
        dupcheck = options.pop('dupcheck')
        if dupcheck:
            checker = LoadFile.dupcheck(file, 'loadfile')
        data = cls.read_file(file, **options.pop('file', {}))
        blk = cls._bulk(data, **options)
        if blk:
            result = await blk.sync_execute()
            if dupcheck:
                checker.save()
        return result