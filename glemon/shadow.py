# 项目：数据库模型
# 模块：配置文件
# 作者：黄涛
# License:GPL
# Email:huangtao.sh@icloud.com
# 创建：2017-10-14 07:58

from orange import *
from .document import *

PASSWORDNAMES={'passwd','password'}   # 定义密码字段名称
chkpwd = lambda x:x.lower() in PASSWORDNAMES

class Shadow(Document):           # 配置库
    _projects='id','profile'  # 标志，具体配置内容

    @classmethod
    def read(cls,zhonglei):
        # 根据标志的名称获取配置内容，如有密码字段则自动解密
        for obj in cls.objects(_id=zhonglei):
            profile=obj.profile.copy()
            for k,v in profile.items():
                if chkpwd(k):
                    profile[k]=decrypt(profile[k])
            return profile
        else:
            return {}

    @classmethod
    def write(cls,zhonglei,profile=None):
        # 设置配置，如有密码字段则自动加密
        if profile:
            profile=profile.copy()
            for k,v in profile.items():
                if chkpwd(k):
                    profile[k]=encrypt(v)
            obj=cls.objects(_id=zhonglei).upsert_one(
                profile=profile)
        else: # 如profile 为空，则删除相应的配置
            cls.objects(_id=zhonglei).delete()
