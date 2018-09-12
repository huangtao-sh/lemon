# 项目：数据库模型
# 模块：数据文件导入检查模块
# 作者：黄涛
# License:GPL
# Email:huangtao.sh@icloud.com
# 创建：2016-05-20 12:26
# 修改：2018-09-12 17:16 新增 dupcheck 功能


from .document import Document, P, FileImported
from orange import Path, POSIX, deprecate


def get(filename):
    file = Path(filename)
    name = str(file)
    if not POSIX:
        name = name.lower()
    return name, int(file.stat().st_mtime)


class LoadFile(Document):
    _projects = 'filename', 'category', 'mtime'

    @classmethod
    def dupcheck(cls, filename, category=None):
        filename, mtime = get(filename)
        obj = cls.objects.filter(category=category, filename=filename).first()
        if not obj:
            obj = cls(filename=filename, category=category, mtime=mtime)
        elif obj.mtime < mtime:
            obj.mtime = mtime
        else:
            raise FileImported(filename)
        return obj

    def done(self):
        super().save()

    @classmethod
    @deprecate('dup_check')
    def check(cls, category, *files):
        results = []
        for file in files:
            filename, mtime = get(file)
            fn = cls.objects.filter(
                category=category, filename=filename).first()
            if(not fn)or fn.mtime < mtime:
                results.append(file)
        return results

    @classmethod
    @deprecate('dup_check')
    def save(cls, category, *files):
        for filename in files:
            filename, mtime = get(filename)
            cls.objects.filter(category=category,
                               filename=filename).upsert_one(mtime=mtime)


dup_check = LoadFile.dupcheck
