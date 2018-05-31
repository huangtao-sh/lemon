# 项目：数据库模型
# 模块：数据库模型
# 作者：黄涛
# License:GPL
# Email:huangtao.sh@icloud.com
# 创建：2018-05-30 20:22

from .document import Document, P
from orange import generator, split
from orange.coroutine import wait


class NewDocument(Document):
    load_kw = {}

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
    def conv_data(cls, rows, header=None, mapper=None, **kw):
        pass
