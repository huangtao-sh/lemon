# 项目：协程版Mogodb
# 模块：数据库模型
# 作者：黄涛
# License:GPL
# Email:huangtao.sh@icloud.com
# 创建：2017-11-13


from orange import Path, decode, split
from orange.coroutine import *
import xlrd


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
        return '文件 %s 已导入数据库，跳过' % (self.filename)


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
        mapper = mapper or (cls._load_mapper and cls._load_mapper.copy())
        header = header or (cls._load_header and cls._load_header.copy())
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
                new_mapper = {}
                for x, y in mapper.items():
                    if isinstance(y, int):
                        new_mapper[x] = y
                    elif isinstance(y, str):
                        if y in row:
                            new_mapper[x] = row.index(y)
                mapper = new_mapper
            data = data[i + 1:]
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
        if not LoadFile.check(cls.__name__, filename):
            raise FileImported(filename)

    @classmethod
    def _importsave(cls, filename):
        from .loadcheck import LoadFile
        LoadFile.save(cls.__name__, filename)

    @classmethod
    def _proc_del(cls, data, **kw):
        rows = []
        for row in data:
            if row:
                row = [x.strip() if isinstance(x, str)
                       else x for x in eval(row)]
            rows.append(row)
        return rows

    @classmethod
    def _proc_csv(cls, data, **kw):
        return [[x.strip() for x in row.split(',')] for row in data if row]

    @classmethod
    def _proc_xls(cls, data, **kw):
        book = xlrd.open_workbook(file_contents=data)
        for index, sheet in enumerate(book.sheets()):
            yield cls._proc_sheet(index=index, name=sheet.name,
                                  data=sheet._cell_values, **kw)

    @classmethod
    def _proc_sheet(cls, index, name, data, **kw):
        return cls, data

    @classmethod
    def import_file(cls, filename, dupcheck=False, drop=False,
                    method='insert', keys='_id', **kw):
        dupcheck and cls._dupcheck(filename)          # 防重复文件检查
        data = _read(str(filename))                   # 读取文件
        proc = FILETYPES.get(Path(filename).lsuffix)  # 获取处理文件器
        if proc == '_proc_xls':
            for c, data in cls._proc_xls(data, **kw):
                if c and data:
                    getattr(c, '_load_data')(data, drop=drop, method=method,
                                             keys=keys, **kw)
        else:                        # 非Excel文件，需要先进行解码
            data = decode(data).splitlines()
            data = getattr(cls, proc)(data, **kw)
            if data:
                cls._load_data(data, drop=drop, method=method, keys=keys, **kw)
                dupcheck and cls._importsave(filename)
                print('导入数据文件%s成功' % (filename))

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
    async def amport_file(cls, filename, dupcheck=False, drop=False,
                          method='insert', keys='_id', **kw):
        dupcheck and cls._dupcheck(filename)          # 防重复文件检查
        data = await _asyncio_read(str(filename))     # 读取文件
        proc = FILETYPES.get(Path(filename).lsuffix)  # 获取处理文件器
        if proc == '_proc_xls':
            for result in cls._proc_xls(data, **kw):
                if result:
                    c, data = result
                    await getattr(c, '_aload_data')(data, drop=drop,
                                                    method=method, keys=keys, **kw)
        else:                        # 非Excel文件，需要先进行解码
            data = decode(data).splitlines()
            data = getattr(cls, proc)(data, **kw)
            if data:
                await cls._aload_data(data, method=method, keys=keys, drop=drop, ** kw)
                dupcheck and cls._importsave(filename)
                print('导入数据文件%s成功' % (filename))

    @classmethod
    async def _aload_data(cls, data, drop=False, method='insert', keys='_id', **kw):
        data = cls._proc_data(data, method=method, keys=keys, **kw)
        if data:
            if drop:
                cls._collection.drop()
            if method == 'insert':
                # await cls.abjects.insert(data)
                data = list(data)
                await wait([cls.abjects.insert(d)for d in split(data)])
            else:
                proc = cls._acollection.update
                upsert = method == 'upsert'
                await wait([proc(f, u, upsert=upsert) for f, u in data])
