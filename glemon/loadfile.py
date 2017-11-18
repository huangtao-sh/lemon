# 项目：协程版Mogodb
# 模块：数据库模型
# 作者：黄涛
# License:GPL
# Email:huangtao.sh@icloud.com
# 创建：2017-11-13


from orange import Path, decode
from orange.coroutine import *


def _read(filename):
    with open(filename, 'rb') as f:
        return f.read()


async def _asyncio_read(filename):
    import aiofiles
    async with aiofiles.open(filename, 'rb')as f:
        return await f.read()


class FileImported(Exception):

    def __init__(self, filename):
        super().__init__()
        self.filename = filename

    def __str__(self):
        return '文件 %s 已导入数据库' % (self.filename)

FILETYPES = {
    '.del': '_proc_del',
    '.xlsx': '_proc_xls',
    '.xls': '_proc_xls',
    '.xlsm': '_proc_xls',
    '.csv': '_proc_csv',
    '.txt': '_proc_txt',
}


class ImportFile(object):
    '''导入文件类，可以做为基类'''
    _load_mapper = None  # 导入数据时的表头，主要用于跳过标题行
    _load_header = None  # 导入数据时的表头，主要用于跳过标题行，
    _projects = None
    _collection = None
    # 可以是一个字段，也可以是多个字段，必须为list或
    # tuple或str

    @classmethod
    def _proc_data(cls, data, fields=None, mapper=None, header=None, keys='_id', method='insert', **kw):
        mapper = mapper or cls._load_mapper
        header = header or cls._load_header
        if isinstance(header, str):
            header = (header,)
        fields = fields or cls._projects
        if(not header)and isinstance(mapper, dict):
            header = [x for x in mapper.values()if isinstance(x, str)]
        if header:
            for i, row in enumerate(data):
                if all(x in row for x in header):
                    break
            if isinstance(mapper, dict):
                new_mapper = {x: row.index(y) for x,
                              y in mapper.items() if isinstance(y, str)}
                mapper.update(new_mapper)
            data = data[i+1:]
        if not mapper:
            mapper = dict(zip(fields, range(len(fields))))
        elif not isinstance(mapper, dict):
            mapper = dict(zip(fields, mapper))
        if method == 'insert':
            return [{k: row[v] for k, v in mapper.items()}for row in data]
        else:
            keys = keys if isinstance(keys, (tuple, list)) else (keys,)
            val_mapper = mapper.copy()
            key_mapper = {key: val_mapper.pop(key) for key in keys}
            def _extract(row):
                return {k: row[v] for k, v in key_mapper.items()},\
                    {k: row[v] for k, v in val_mapper.items()},
            return (_extract(row)for row in data)

    @classmethod
    def _dupcheck(cls, filename):
        from .loadcheck import LoadFile
        if LoadFile.check(cls.__name__, filename):
            raise FileImported(filename)

    @classmethod
    def _proc_del(cls, data, **kw):
        return [eval(x)for x in data if x]

    @classmethod
    def import_file(cls, filename, dupcheck=False, drop=False, method='insert', keys='_id', **kw):
        dupcheck and cls._dupcheck(filename)          # 防重复文件检查
        data = _read(str(filename))                   # 读取文件
        proc = FILETYPES.get(Path(filename).lsuffix)  # 获取处理文件器
        if proc != 'proc_xls':                        # 非Excel文件，需要先进行解码
            data = decode(data)
            data = data.splitlines()
        data = getattr(cls, proc)(data, **kw)
        cls._load_data(data, drop=drop, method=method, keys=keys, **kw)

    @classmethod
    def _load_data(cls, data, drop=False, method='insert', keys='_id', **kw):
        data = cls._proc_data(data, method=method, keys=keys, **kw)
        if data:
            if drop:
                cls._collection.drop()
            if method == 'insert':
                cls.objects.insert(data)
            else:
                upsert = True if method == 'upsert' else False
                for f, u in data:
                    cls._collection.update_one(f, {'$set': u}, upsert=upsert)

    @classmethod
    async def amport_file(cls, filename, dupcheck=True, drop=False, method='insert', keys='_id', **kw):
        drop = clear or drop
        dupcheck = dupcheck and dup_check
        dupcheck and cls._dupcheck(filename)          # 防重复文件检查
        data = await _asyncio_read(str(filename))     # 读取文件
        proc = FILETYPES.get(Path(filename).lsuffix)  # 获取处理文件器
        if proc != 'proc_xls':                        # 非Excel文件，需要先进行解码
            data = decode(data)
            data = data.splitlines()
        data = getattr(cls, proc)(data, **kw)
        await cls._aload_data(data, method=method, keys=keys, **kw)

    @classmethod
    async def _aload_data(cls, data, drop=False, method='insert', keys='_id', **kw):
        data = cls._proc_data(data, method=method, keys=keys, **kw)
        if data:
            if drop:
                cls._collection.drop()
            if method == 'insert':
                await cls.abjects.insert_one(data)
            else:
                proc = cls._acollection.update
                upsert = method == 'upsert'
                await wait([proc(f, u, upsert=upsert) for f, u in data])
