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
    async def _insert(cls, data, drop=False, inorder=True, projects=None, **kw):
        projects = projects or cls._projects

        def _insert(d):
            return cls.ansert_many(tuple(map(lambda x: dict(zip(projects, x)), d)))
        if drop:
            cls.drop()
        if isinstance(data, generator):
            data = tuple(data)
        if inorder:
            [await _insert(d)for d in split(data, 10000)]
        else:
            await wait(tuple(map(_insert, split(data, 10000))))

    @classmethod
    async def _update(cls,data,projects=None,keys=None,**kw):
        pass