# 项目：数据库模型
# 模块：配置文件
# 作者：黄涛
# License:GPL
# Email:huangtao.sh@icloud.com
# 创建：2017-10-14 07:58
# 修订：2018-09-11 新增 shadow 功能

from orange import encrypt, decrypt
from .document import Document

PASSWORDNAMES = {'passwd', 'password'}   # 定义密码字段名称


def chkpwd(x): return x.lower() in PASSWORDNAMES


class Shadow(Document):           # 配置库
    _projects = 'id', 'profile'  # 标志，具体配置内容

    @classmethod
    def load(cls, name):
        return cls.read(name)

    @classmethod
    def dump(cls, obj, name):
        return cls.write(name, obj)

    @classmethod
    def read(cls, zhonglei):
        # 根据标志的名称获取配置内容，如有密码字段则自动解密
        obj = cls.objects(_id=zhonglei).first()
        profile = obj.profile
        if isinstance(profile, dict):
            profile = obj.profile.copy()
            for k, v in profile.items():
                if chkpwd(k):
                    profile[k] = decrypt(profile[k])
        return profile

    @classmethod
    def write(cls, zhonglei, profile=None):
        # 设置配置，如有密码字段则自动加密
        if profile:
            if isinstance(profile, dict):
                profile = profile.copy()
                for k, v in profile.items():
                    if chkpwd(k):
                        profile[k] = encrypt(v)
            cls.objects(_id=zhonglei).upsert_one(
                profile=profile)
        else:  # 如profile 为空，则删除相应的配置
            cls.objects(_id=zhonglei).delete()
