# 项目：数据库模型
# 模块：数据文件导入检查模块
# 作者：黄涛
# License:GPL
# Email:huangtao.sh@icloud.com
# 创建：2016-05-20 12:26

from .document import *


def get(filename):
    file = Path(filename)
    return str(file.resolve()), int(file.stat().st_mtime)


class LoadFile(Document):
    _projects = 'filename', 'category', 'mtime'

    @classmethod
    def check(cls, category, *files):
        results = []
        for file in files:
            filename, mtime = get(file)
            fn = cls.objects(category=category, filename=filename).\
                first()
            if(not fn)or fn.mtime < mtime:
                results.append(file)
        return results

    @classmethod
    def save(cls, category, *files):
        for filename in files:
            filename, mtime = get(filename)
            cls.objects(category=category, filename=filename).\
                upsert_one(mtime=mtime)
