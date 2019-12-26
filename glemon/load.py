# 项目：   MongoDB 数据库
# 模块：   文件导入模块
# 作者：   黄涛
# License: GPL
# Email:   huangtao.sh@icloud.com
# 创建：2019-12-21 17:18

from glemon.document import Document, P
from orange import Path, Data, limit, R, extract
from .bulk import BulkWrite
from .loadcheck import LoadFile
pattern = R / r'(\d{6,8}|\d{4}-\d{2})'


class LoadDocument(Document):
    load_options = {
    }  # file :reader: callable,encoding,errors,columns,converter,delimter,sep

    @classmethod
    def get_ver(cls):
        name = LoadFile.find(P.category == cls.__name__).order_by(
            -P.mtime).limit(1).scalar('filename')
        if name:
            return extract(name, pattern)

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
        dupcheck = options.pop('dupcheck', False)
        if dupcheck:
            checker = LoadFile.dupcheck(file, cls.__name__)

        data = cls.read_file(file, **options.pop('file', {}))
        blk = cls._bulk(data, **options)
        if blk:
            if dry:
                for x in blk:
                    print(x)
            else:
                result = blk.execute()
                if dupcheck:
                    checker.done()
                return result

    @classmethod
    async def sync_load_file(cls, file, options=None):
        options = options or cls.load_options
        dupcheck = options.pop('dupcheck', False)
        if dupcheck:
            try:
                checker = LoadFile.dupcheck(file, cls.__name__)
            except:
                print(f'{file.name} 已导入，忽略')
                return
        data = cls.read_file(file, **options.pop('file', {}))
        blk = cls._bulk(data, **options)
        if blk:
            result = await blk.sync_execute()
            if dupcheck:
                checker.done()
        return result
