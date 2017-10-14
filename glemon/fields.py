# 项目：数据库模型
# 模块：数据库字段
# 作者：黄涛
# License:GPL
# Email:huangtao.sh@icloud.com
# 创建：2016-12-28 10:43

class BaseField(object):
    __fields_count=0
    def __init__(self,name=None):
        BaseField.__fields_count+=1
        self._create_order=BaseField.__fields_count
        self.name=name

    def __set__(self,instance,val):
        if instance is not None:
            instance[self.name]=val
        
    def __get__(self,instance,cls):
        if instance:
            return instance.get(self.name)
        else:
            return cls.__dict__.get(self.name)

    def __repr__(self):
        return '%s(%s)'%(self.__class__,self.name)

class IDField(BaseField):
    pass

class StringField(BaseField):
    pass

class IntField(BaseField):
    pass

class DateTimeField(BaseField):
    pass

class ListField(BaseField):
    pass

class EmbeddedDocumentField(BaseField):
    pass

class DictField(BaseField):
    pass
