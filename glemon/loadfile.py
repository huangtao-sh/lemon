# 项目：协程版Mogodb
# 模块：数据库模型
# 作者：黄涛
# License:GPL
# Email:huangtao.sh@icloud.com
# 创建：2017-11-13

from glemon.loadcheck import LoadFile
from orange import Path, decode
from orange.coroutine import *

methods = set(['insert', 'upsert', 'update'])


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


def _split(data, keys='_id'):
    if isinstance(keys, str):
        key = {keys: data.pop(keys)}
    else:
        key = {k: data.pop(k)for k in keys}
    return key, data


class Objects():

    def insert(self, data):
        for d in data:
            print(d)

    def __call__(self, **d):
        print('query:', d)
        return self

    def upsert(self, **d):
        print('upsert:')
        print(d)

    def update(self, **d):
        print('update')
        print(d)


class Abjects():

    async def insert(self, data):
        for d in data:
            print(d)

    def __call__(self, **d):
        print('query:', d)
        return self

    async def update(self, **d):
        print('update')
        print(d)


class ImportFile(object):
    '''导入文件类，可以做为基类'''
    _load_mapper = None  # 导入数据时的表头，主要用于跳过标题行
    _load_header = None  # 导入数据时的表头，主要用于跳过标题行，
    _projects = None
    _collection = None
    # 可以是一个字段，也可以是多个字段，必须为list或
    # tuple或str
    objects = Objects()
    abjects = Abjects()

    @classmethod
    def _proc_data(cls, data, fields=None, mapper=None, header=None, **kw):
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
                mapper = [mapper[x] for x in fields]
            data = data[i+1:]
        datas = []
        field_count = len(mapper) if mapper else len(fields)

        def add(row):
            d = cls._check_row(row)
            if d:
                datas.append(dict(zip(fields, d)))
        if mapper:
            [add([row[x] for x in mapper]) for row in data
             if len(row) >= field_count]
        else:
            [add(row) for row in data if len(row) >= field_count]
        return datas

    @classmethod
    def _check_row(cls, row):
        return row

    @classmethod
    def __dupchek(cls, filename):
        if LoadFile.check(cls.__name__, filename):
            raise FileImported(filename)

    @classmethod
    def _proc_del(cls, data, **kw):
        return [eval(x)for x in data if x]

    @classmethod
    def import_file(cls, filename, dupcheck=False, drop=False, method='insert', keys='_id', **kw):
        dupcheck and cls.__dupcheck(filename)  # 防重复文件检查
        data = _read(str(filename))          # 读取文件
        proc = FILETYPES.get(Path(filename).lsuffix)  # 获取处理文件器
        if proc != 'proc_xls':           # 非Excel文件，需要先进行解码
            data = decode(data)
            data = data.splitlines()
        data = getattr(cls, proc)(data, **kw)
        data = cls._proc_data(data, **kw)
        if data:
            if drop:
                cls._collection.drop()
            if method == 'insert':
                cls.objects.insert(data)
            else:
                upsert = True if method == 'upsert' else False
                for row in data:
                    key, value = _split(row, keys)
                    cls.objects(**key).update(upsert=upsert, **value)

    @classmethod
    async def amport_file(cls, filename, dupcheck=False, drop=False, method='insert', keys='_id', **kw):
        dupcheck and cls.__dupcheck(filename)  # 防重复文件检查
        data = await _asyncio_read(str(filename))          # 读取文件
        proc = FILETYPES.get(Path(filename).lsuffix)  # 获取处理文件器
        if proc != 'proc_xls':           # 非Excel文件，需要先进行解码
            data = decode(data)
            data = data.splitlines()
        data = getattr(cls, proc)(data, **kw)
        data = cls._proc_data(data, **kw)
        if data:
            if drop:
                cls._collection.drop()
            if method == 'insert':
                await cls.abjects.insert(data)
            else:
                upsert = True if method == 'upsert' else False
                for row in data:
                    key, value = _split(row, keys)
                    await cls.abjects(**key).update(upsert=upsert, **value)
            return len(data)


if __name__ == '__main__':
    f = Path(r'~\OneDrive\工作\参数备份\运营管理2017-10\shendawei\ggbzb.del')
    a = ImportFile()
    run(a.amport_file(f, fields='a,b,c'.split(','), keys=('a', 'b'),
                      mapper=(0, 1, 2), method='upsert'))
