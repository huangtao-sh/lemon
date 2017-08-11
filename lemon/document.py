# 项目：协程版Mogodb
# 模块：数据库模型
# 作者：黄涛
# License:GPL
# Email:huangtao.sh@icloud.com
# 创建：2017-07-22 09:52

from pymongo import *
from .query import *
from .config import *

class DocumentMeta(type):
    _db_cache={}
    _collection_cache={}
    @property
    def objects(cls):
        return BaseQuery(cls)
    
    @property
    def abjects(cls):
        return AsyncioQuery(cls)

    def drop(cls):
        return cls._collection.drop()

    drop_collection=drop
    
    def aggregate(cls,*args,**kw):
        return Aggregation(cls,*args,**kw)
    
    def insert_one(cls,*args,**kw):
        return cls._collection.insert_one(*args,**kw)

    def insert_many(cls,*args,**kw):
        return cls._collection.insert_many(*args,**kw)

    def ansert_one(cls,*args,**kw):
        return cls._acollection.insert_one(*args,**kw)

    def ansert_many(cls,*args,**kw):
        return cls._acollection.insert_many(*args,**kw)

class Document(dict,metaclass=DocumentMeta):
    __db=None
    __adb=None
    _projects=()
    _textfmt=''    # 文本格式
    _htmlfmt=''    # 超文本格式
    _load_mapper=None  # 导入数据时的表头，主要用于跳过标题行
    _load_header=None  # 导入数据时的表头，主要用于跳过标题行，
                       # 可以是一个字段，也可以是多个字段，必须为list或
                       # tuple或str
    
    @classmethod
    def load_files(cls,*files,clear=False,dup_check=True,**kw):
        '''通过文件批量导入数据
        files: 导入文件清单
        clear：清理原表中的数据，默认为不清理
        dup_check：重复导入检查，默认为检查
        kw：其他参数
        '''
        from .loadcheck import LoadFile
        if dup_check and clear:
            # 如果检查重复为真，则检查文件
            files=LoadFile.check(cls.__name__,*files)
        if files and clear:
            # 导入前清理数据
            cls.objects.delete()
        for filename in files:
            # 处理文件
            filename = Path(filename)
            kw['filename']=filename
            ext = filename.lsuffix
            if ext=='.del':
                cls.load_data([d for d in filename], **kw)
            elif ext in ('.txt','.csv'):
                cls.load_txtfile(**kw)
            elif ext.startswith('.xls'):
                for index, sheetname, data in filename.iter_sheets():
                    if data:
                        cls.load_sheet(data=data,sheetname=sheetname,
                                       index=index,**kw)
        if dup_check and clear:
            LoadFile.save(cls.__name__,*files)
            
    @classmethod
    def load_sheet(cls,data,sheetname,index,**kw):
        # 逐表处理excel文件
        cls.load_data(data,**kw)
        
    @classmethod
    def load_txtfile(cls,filename,sep=',',**kw):
        # 处理文本文件
        data = [d.split(sep) for d in filename.lines]
        cls.load_data(data,**kw)

    @classproperty
    def _fields_without_id(cls):
        # 查询除id以外的字段名
        return self._projects

    @classmethod
    def _check_row(cls,row):
        return row
    
    @classmethod
    def _batch_insert(cls,data):
        cls.objects.insert(data)
    
    @classmethod
    def load_data(cls,data,fields=None,mapper=None,quote=None,
                  header=None,**kw):
        # 批量导入数据
        def extract_str(x):
            if isinstance(x,str)and x.startswith(quote)and \
              x.endswith(quote):
                x=x[1:-1].strip()
            return x
        mapper=mapper or cls._load_mapper
        header=header or cls._load_header
        if isinstance(header,str):
            header=(header,)
        fields=fields or cls._fields_without_id
        if(not header)and isinstance(mapper,dict):
            header=[x for x in mapper.values()if isinstance(x,str)]

        if header:
            for i,row in enumerate(data):
                if all(x in row for x in header):
                    break
            if isinstance(mapper,dict):
                new_mapper={x:row.index(y) for x,
                            y in mapper.items() if isinstance(y,str)}
                mapper.update(new_mapper)
                mapper=[mapper[x] for x in fields]
            data=data[i+1:]
        datas=[]
        fields=[cls._translate_field_name(x) for x in fields]
        field_count =len(mapper) if mapper else len(fields)
        def add(row):
            d = cls._check_row(row)
            if d:
                datas.append(dict(zip(fields, d)))
        if mapper:
            [add([row[x] for x in mapper]) for row in data\
             if len(row)>=field_count]
        else:
            [add(row) for row in data if len(row)>=field_count]
        if datas:
            cls._batch_insert(datas)

    @property
    def id(self):
        return self.get('_id')

    @id.setter
    def id(self,value):
        self['_id']=value

    def save(self):
        if self._modified:
            if self.id:
                d=self.copy()
                d.pop('_id')
                self.__class__.objects(P.id==self.id).upsert_one(**d)
            else:
                self._collection.insert_one(self)
            self._modified=False

    def __setitem__(self,*args,**kw):
        self._modified=True
        return super().__setitem__(*args,**kw)
    
    @property
    def _text(self):
        # 返回本实例的文本格式
        return self._textfmt.format(self=self)

    def __str__(self):
        # 返回本实例的文本格式
        return self._text if self._textfmt else super().__str__()
    
    @property
    def _html(self):
        # 返回本实例的超文本格式
        return self._htmlfmt.format(self=self)

    def __init__(self,*args,id=None,from_query=False,**kw):
        self._modified=not from_query
        if id:
            kw['_id']=id
        super().__init__(*args,**kw)
        
    @cachedproperty
    def _acollection(cls):
        if Document.__adb is None:
            from motor.motor_asyncio import AsyncIOMotorClient
            client=AsyncIOMotorClient(**config())
            Document.__adb=client.get_database()
        return Document.__adb[convert_cls_name(cls.__name__)]
    
    @cachedproperty
    def _collection(cls):
        if Document.__db is None:
            client=MongoClient(**config())
            Document.__db=client.get_database()
        return Document.__db[convert_cls_name(cls.__name__)]

    def values(self,*fields):
        return tuple((getattr(self,p) for p in fields))

    def __getattr__(self,attr):
        return self.get(attr) if attr in self._projects else \
            super().getattr(attr)

    def __setattr__(self,name,value):
        if name in self._projects:
            self[name]=value
        else:
            super().__setattr__(name,value)
