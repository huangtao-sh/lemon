# 项目：数据库模型
# 模块：数据库模型
# 作者：黄涛
# License:GPL
# Email:huangtao.sh@icloud.com
# 创建：2018-05-30 20:22

from .document import Document, P
from orange import generator, split, Path, decode
from orange.coroutine import wait
from .loadcheck import LoadFile
from aiofiles import open
import xlrd


class FileImported(Exception):

    def __init__(self, filename):
        super().__init__()
        self.filename = filename

    def __str__(self):
        return '文件 %s 已导入数据库，跳过' % (self.filename)


class NewDocument(Document):
    loadfile_kw = {}

    @classmethod
    async def _insert(cls, rows, drop=False, inorder=True, projects=None, **kw):
        projects = projects or cls._projects

        def _insert(d):
            return cls.ansert_many(tuple(map(lambda x: dict(zip(projects, x)), d)))
        if drop:
            cls.drop()
        if isinstance(rows, generator):
            rows = tuple(rows)
        if inorder:
            [await _insert(d)for d in split(rows, 10000)]
        else:
            await wait(tuple(map(_insert, split(rows, 10000))))

    @classmethod
    async def _update(cls, rows, projects=None, keys="_id", multi=False, upsert=False, **kw):
        if isinstance(keys, str):
            keys = [keys]
        keys = set(keys)
        projects = projects or cls._projects
        update = cls._acollection.update_many if multi else cls._acollection.update_one

        def mk_obj(row):
            key, value = {}, {}
            for name, v in zip(projects, row):
                if name in keys:
                    key[name] = v
                else:
                    value[name] = v
            return update(key, {'$set': value}, upsert=upsert)
        await wait(tuple(map(mk_obj, rows)))

    @classmethod
    async def load_data(cls, rows, header=None, mapper=None, **kw):
        pass

    @classmethod
    async def load_file(cls, filename, encoding='auto', dupcheck=False, **kwargs):
        kw = cls.loadfile_kw.copy()
        kw.update(kwargs)
        filename = Path(filename)
        async with open(str(filename), 'rb')as f:
            data = await f.read()
        if dupcheck and not LoadFile.check(cls.__name__, filename):
            raise FileImported(filename.name)
        if filename.lsuffix.startswith('.xls'):
            book = xlrd.open_workbook(file_contents=data)
            for index, sheet in enumerate(book.sheets()):
                rows = sheet._cell_values
                if rows:
                    await cls.load_data(rows, sheetindex=index, sheet=sheet, **kw)
        else:
            if encoding == 'auto':
                rows = decode(rows)
            elif encoding == None:
                pass
            else:
                rows = rows.decode(encoding)
            await cls.load_data(rows, suffix=filename.suffix, **kw)
        if dupcheck:
            LoadFile.save(cls.__name__, filename)
