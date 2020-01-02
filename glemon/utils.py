# 项目：   Pymongo 数据库封装
# 模块：   工具模块
# 作者：   黄涛
# License: GPL
# Email:   huangtao.sh@icloud.com
# 创建：2020-01-02 20:06


def enlist(fields):
    return fields.split(',') if isinstance(fields, str) else fields
