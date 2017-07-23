# 项目：数据库模块
# 模块：查询模块
# 作者：黄涛
# License:GPL
# Email:huangtao.sh@icloud.com
# 创建：2016-12-28 15:14

from pymongo import *
from .expr import *
from .paginate import *

def abort(*args,**kwargs):
    import flask
    flask.abort(*args,**kwargs)

class BaseQuery(object):
    def __init__(self,document):
        self.document=document
        self._query=[]
        self._skip=0
        self._limit=0
        self._sort=[]
        self._projections=[]
        
    @property
    def collection(self):
        return self.document._collection

    @property
    def query(self):
        _query=[query.to_query() if hasattr(query,'to_query') else query for query in self._query]
        if len(_query)==1:
            query=_query[0]
        elif len(_query)>1:
            query={'$and':_query}
        else:
            query={}
        return query

    async def first(self):
        obj=await self.collection.find_one(self.query,skip=self._skip,
                projection=self.projection,sort=self._sort)
        return obj and self.document(from_query=True,**obj)

    def first_or_404(self):
        obj=self.first()
        return obj or abort(404)

    def delete(self):
        self.collection.delete_many(self.query)

    def delete_one(self):
        self.collection.delete_one(self.query)
        
    @property
    def projection(self):
        projections=None
        if self._projections:
            projections={}
            for p in self._projections:
                p=P(p) if isinstance(p,str) else p
                projections.update(p.to_project())
        return projections
    
    @property
    def cursor(self):
        return self.collection.find(self.query,skip=self._skip,
                projection=self.projection,sort=self._sort,limit=self._limit)
    
    def order_by(self,*projections):
        for p in projections:
            if isinstance(p,str):
                p=P(p)
            self._sort.append(p.to_order())
        return self

    def project(self,*projections):
        for p in projections:
            if isinstance(p,str):
                p=P(p)
            self._projections.append(p)
        return self
    
    def filter(self,*query,**kw):
        self._query.extend(query)
        if kw:
            self._query.append(kw)
        return self

    __call__ = filter
    
    def skip(self,i):
        self._skip=i
        return self

    def limit(self,i):
        self._limit=i
        return self

    async def __aiter__(self):
        async for obj in self.cursor:
            yield self.document(from_query=True,**obj)

    async def get(self,id):
        from bson.objectid import ObjectId
        try:
            id=ObjectId(id)
        except:
            pass
        obj=await self.collection.find_one({'_id':id})
        return obj and self.document(from_query=True,**obj)

    async def get_or_404(self,id):
        r=await self.get(id)
        return r or abort(404)

    async def scalar(self,*fields):
        self._projections=fields
        if len(fields)==1:
            async for i in self:
                yield getattr(i,fields[0])
        else:
            async for i in self:
                yield tuple(getattr(i,x) for x in fields)

    values_list = scalar

    def distinct(self,*args):
        return self.cursor.distinct(*args)

    def count(self,with_limit_and_skip=False):
        return self.cursor.count(with_limit_and_skip)

    def rewind(self):
        return self.cursor.rewind()
        
    async def paginate(self,page, per_page=20):
        total=await self.cursor.count(True)
        pages,m=divmod(total,per_page)
        if m:
            pages+=1
        ensure((page>0)and(page<=pages),'页码超限！')
        skip,limit=self._skip,self._limit # 保存当前状态
        self._skip=self._skip+(page-1)*per_page
        self._limit=per_page
        self.rewind()
        items=[]
        async for i in self:
            items.append(i)
        self._skip,self._limit=skip,limit  # 恢复当原状态
        return Pagination(self, page, per_page, total, items)

    def update(self,*args,upsert=False, multi=True, **kw):
        func=self.collection.update_many if multi else self.collection.update_one
        args=[arg.to_update()for arg in args]
        for k,v in kw.items():
            if k.startswith('$'):
                args.append({k:v})
            else:
                args.append({'$set':{k:v}})
        updater={}
        for arg in args:
            for k,v in arg.items():
                if k in updater:
                    a=updater.get(k)
                    if isinstance(a,dict) and isinstance(v,dict):
                        a.update(v)
                    else:
                        updater[k]=v
                else:
                    updater[k]=v
        return func(self.query,updater,upsert=upsert)

    def upsert(self,*args,**kw):
        kw['upsert']=True
        return self.update(*args,**kw)

    def update_one(self,*args,**kw):
        kw['multi']=False
        return self.update(*args,**kw)

    def upsert_one(self,*args,**kw):
        kw['multi']=False
        kw['upsert']=True
        return self.update(*args,**kw)
        
class Aggregation:
    def __init__(self,document,pipeline=None,**kw):
        self.collection=document._collection
        self.pipeline=pipeline or []
        self.kw=kw or {}

    def __aiter__(self):
        '''迭代返回所有值'''
        return self.collection.aggregate(self.pipeline,**self.kw)

    async def paginate(self, page, per_page=20, error_out=True):
        """Returns `per_page` items from page `page`.  By default it will
        abort with 404 if no items were found and the page was larger than
        1.  This behavor can be disabled by setting `error_out` to `False`.

        Returns an :class:`Pagination` object.
        """
        if error_out and page < 1:
            abort(404)
        pipeline=self.pipeline.copy()
        pipeline.append({'$skip':(page-1)*per_page})
        items=[]
        async for obj in self.collection.aggregate(pipeline,**self.kw):
            items.append(obj)
        # items=[i for i in \
        #       self.collection.aggregate(pipeline,**self.kw)]
        total=(page-1)*per_page+len(items)
        items=items[:per_page]
        if not items and page != 1 and error_out:
            abort(404)
   
        # No need to count if we're on the first page and there are fewer
        # items than we expected.
        return Pagination(self, page, per_page, total, items)

    def project(self,*args,**kw):
        '''过滤字段'''
        for arg in args:
            kw[arg._name]=not arg._neg
        self.pipeline.append({'$project':kw})
        return self

    def match(self,kw):
        '''条件过滤'''
        if hasattr(kw,'to_query'):
            kw=kw.to_query()
        self.pipeline.append({'$match':kw})
        return self

    def unwind(self,projection):
        '''打开列表字段'''
        if isinstance(projection,Projection):
            projection=projection._name
        if not projection.startswith('$'):
            projection='$%s'%(projection)
        self.pipeline.append({'$unwind':projection})
        return self
    
    def group(self,*args):
        '''聚合字段'''
        _id={}
        kw={}
        for project in args:
            if isinstance(project,P):
                _id.update({project._name:"$%s"%(project._name)})
            else:
                kw.update(project.to_group())
        kw['_id']=_id.popitem()[-1] if len(_id)==1 else _id
        self.pipeline.append({'$group':kw})
        return self

    def sort(self,*args,**kw):
        '''对字段进行排序'''
        for arg in args:
            kw[arg._name]=-1 if arg._neg else 1
        self.pipeline.append({'$sort':kw})
        return self

    def skip(self,num):
        '''跳过记录'''
        self.pipeline.append({'$skip':num})
        return self

    def limit(self,num):
        '''限制输出记录'''
        self.pipeline.append({'$limit':num})
        return self

    def export(self,filename,sheetname='sheet1',range_="A1",columns=None,\
               mapper=None):
        '''导出数据'''
        pass
