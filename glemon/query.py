# 项目：数据库模块
# 模块：查询模块
# 作者：黄涛
# License:GPL
# Email:huangtao.sh@icloud.com
# 创建：2016-12-28 15:14

# from pymongo import *
from .expr import P
from .paginate import Pagination
from orange.coroutine import wait
from orange import ensure, tprint
import math


def abort(*args, **kwargs):
    import flask
    flask.abort(*args, **kwargs)


def _split(data, step=1000):
    length = len(data)
    i = 0
    for i in range(step, length, step):
        yield data[i - step:i]
    else:
        yield data[i:]


class BaseQuery(object):
    '''查询基类'''

    def __init__(self, document):
        '''初始化'''
        self.document = document
        self._query = []
        self._projection = None
        self._skip = 0
        self._limit = 0
        self._sort = []
        self._modified = True
        self._filter = {}
        self._per_page = 0
        self._pages = 0

    @property
    def collection(self):
        '''数据集'''
        return self.document._collection

    @property
    def is_paginated(self):
        return self.pages > 1

    @property
    def items(self):
        return self

    def iter_pages(self, left_edge=2, left_current=2,
                   right_current=5, right_edge=2):
        """Iterates over the page numbers in the pagination.  The four
        parameters control the thresholds how many numbers should be produced
        from the sides.  Skipped page numbers are represented as `None`.
        This is how you could render such a pagination in the templates:

        .. sourcecode:: html+jinja

            {% macro render_pagination(pagination, endpoint) %}
              <div class=pagination>
              {%- for page in pagination.iter_pages() %}
                {% if page %}
                  {% if page != pagination.page %}
                    <a href="{{ url_for(endpoint, page=page) }}">{{ page }}</a>
                  {% else %}
                    <strong>{{ page }}</strong>
                  {% endif %}
                {% else %}
                  <span class=ellipsis>…</span>
                {% endif %}
              {%- endfor %}
              </div>
            {% endmacro %}
        """
        last = 0
        for num in range(1, self.pages + 1):
            if num <= left_edge or \
               (num > self._page - left_current - 1 and
                num < self._page + right_current) or \
               num > self.pages - right_edge:
                if last + 1 != num:
                    yield None
                yield num
                last = num

    @property
    def pages(self):
        return self._pages

    @property
    def per_page(self):
        return self._per_page

    def paginate(self, page=1, per_page=0):
        if per_page > 1:
            self._per_page = per_page
            count = self.count(True)
            self._pages = math.ceil(count / per_page)
        if page >= 1:
            ensure(((page >= 1) and (page <= self.pages)), "页码超限！")
            self._page = page
        self._modified = True
        return self

    def cache(self, **kw):
        from glemon.cachedquery import CachedQuery
        obj = CachedQuery.dumps(self, **kw)
        return str(obj._id)

    def dumps(self):
        d = {name: self.__dict__.get(name) for name in
             ('_skip', '_projection', '_sort', '_limit', '_per_page', '_pages')}
        d['_filter'] = self.query
        return d

    @property
    def query(self):
        '''返回查询条件'''
        if (not self._filter)and self._query:
            _query = [query.to_query() if hasattr(query, 'to_query') else
                      query for query in self._query]
            if len(_query) == 1:
                query = _query[0]
            elif len(_query) > 1:
                query = {'$and': _query}
            else:
                query = {}
            self._filter = query
        return self._filter

    def first(self):
        '''符合条件的第一条记录'''
        obj = self.collection.find_one(self.query, skip=self._skip,
                                       projection=self._projection, sort=self._sort)
        return obj and self.document(from_query=True, **obj)

    def first_or_404(self):
        '''符合条件的第一条记录，如查找不到返回404页面'''
        return self.first() or abort(404)

    def delete(self):
        '''删除符合条件的所有记录'''
        return self.collection.delete_many(self.query)

    def delete_one(self):
        '''删除符合条件的第一条记录'''
        return self.collection.delete_one(self.query)

    def project(self, *projections):
        '''设置Porject'''
        self._modified = True
        self._projection = {}
        for p in projections:
            p = P(p) if isinstance(p, str) else p
            self._projection.update(p.to_project())
        return self

    @property
    def cursor(self):
        '''返回数据游标'''
        if self._modified:
            skip, limit = self._skip, self._limit
            if self.is_paginated:
                skip += (self._page - 1) * self.per_page
                if limit:
                    limit -= (self._page - 1) * self.per_page
                    limit = min(limit, self.per_page)
                else:
                    limit = self.per_page
            self._cursor = self.collection.find(self.query,
                                                skip=skip,
                                                projection=self._projection,
                                                sort=self._sort,
                                                limit=limit)
            self._modified = False
        return self._cursor

    def order_by(self, *projections):
        '''设置排序'''
        self._modified = True
        for p in projections:
            if isinstance(p, str):
                p = P(p)
            self._sort.append(p.to_order())
        return self

    def filter(self, *query, **kw):
        '''增加查询条件'''
        self._filter = None
        self._query.extend(query)
        if kw:
            self._query.append(kw)
        return self

    __call__ = filter

    def skip(self, i):
        self._modified = True
        self._skip = i
        return self

    def limit(self, i):
        self._modified = True
        self._limit = i
        return self

    def __iter__(self):
        for obj in self.cursor:
            yield self.document(from_query=True, **obj)

    def get(self, id):
        from bson.objectid import ObjectId
        try:
            id = ObjectId(id)
        except:
            pass
        obj = self.collection.find_one({'_id': id})
        return obj and self.document(from_query=True, **obj)

    def scalar(self, *fields):
        from .loadfile import enlist
        if len(fields) == 1:
            fields = enlist(fields[0])
        if len(fields) == 1:
            def extract(d): return d.values(*fields)[0]
        else:
            def extract(d): return d.values(*fields)
        for d in self:
            yield extract(d)

    def show(self, *fields, format_spec={}, sep=' '):
        tprint(self.scalar(*fields), format_spec=format_spec, sep=sep)

    values_list = scalar

    def distinct(self, *args):
        return self.cursor.distinct(*args)

    def count(self, with_limit_and_skip=False):
        if self._modified:
            #self._count = self.cursor.count(with_limit_and_skip)
            if with_limit_and_skip:
                kw = {'limit': self._limit,
                      'skip': self._skip}
            else:
                kw = {}
            self._count = self.collection.count_documents(self.query, **kw)
        return self._count

    def __bool__(self):
        '''判断是查询结果是否有记录'''
        return bool(self.count())

    def rewind(self):
        return self.cursor.rewind()

    def _paginate(self, page, per_page=20):
        total = self.cursor.count(True)
        pages, m = divmod(total, per_page)
        if m:
            pages += 1
        ensure((page > 0)and(page <= pages), '页码超限！')
        skip, limit = self._skip, self._limit  # 保存当前状态
        self._skip = self._skip + (page - 1) * per_page
        self._limit = per_page
        self.rewind()
        items = list(self)
        self._skip, self._limit = skip, limit  # 恢复当原状态
        return Pagination(self, page, per_page, total, items)

    def update(self, *args, upsert=False, multi=True, **kw):
        func = self.collection.update_many if multi else \
            self.collection.update_one
        args = [arg.to_update()for arg in args]
        for k, v in kw.items():
            if k.startswith('$'):
                args.append({k: v})
            else:
                args.append({'$set': {k: v}})
        updater = {}
        for arg in args:
            for k, v in arg.items():
                if k in updater:
                    a = updater.get(k)
                    if isinstance(a, dict) and isinstance(v, dict):
                        a.update(v)
                    else:
                        updater[k] = v
                else:
                    updater[k] = v
        return func(self.query, updater, upsert=upsert)

    def upsert(self, *args, **kw):
        kw['upsert'] = True
        return self.update(*args, **kw)

    def update_one(self, *args, **kw):
        kw['multi'] = False
        return self.update(*args, **kw)

    def upsert_one(self, *args, **kw):
        kw['multi'] = False
        kw['upsert'] = True
        return self.update(*args, **kw)

    def _insert(self, objs, func=None, **kw):
        return self.collection.insert_many(map(func, objs)
                                           if func else objs, **kw)

    def insert(self, objs, func=None, **kw):
        return sum([len(self._insert(data, func=func, **kw).inserted_ids)
                    for data in _split(objs)])


class AsyncioQuery(BaseQuery):

    async def paginate(self, page=1, per_page=0):
        if per_page > 1:
            self._per_page = per_page
            count = await self.count(True)
            self._pages = math.ceil(count / per_page)
        if page >= 1:
            ensure(((page >= 1) and (page <= self.pages)), "页码超限！")
            self._page = page
        self._modified = True
        return self

    @property
    def collection(self):
        return self.document._acollection

    async def first(self):
        obj = await self.collection.find_one(self.query, skip=self._skip,
                                             projection=self._projection, sort=self._sort)
        return obj and self.document(from_query=True, **obj)

    async def __aiter__(self):
        async for obj in self.cursor:
            yield self.document(from_query=True, **obj)

    async def get(self, id):
        from bson.objectid import ObjectId
        try:
            id = ObjectId(id)
        except:
            pass
        obj = await self.collection.find_one({'_id': id})
        return obj and self.document(from_query=True, **obj)

    async def scalar(self, *fields):
        self.project(*fields)
        if len(fields) == 1:
            def extract(d): return d.values(*fields)[0]
        else:
            def extract(d): return d.values(*fields)
        async for i in self:
            yield extract(i)

    async def _paginate(self, page, per_page=20):
        total = await self.cursor.count(True)
        pages, m = divmod(total, per_page)
        if m:
            pages += 1
        ensure((page > 0)and(page <= pages), '页码超限！')
        skip, limit = self._skip, self._limit  # 保存当前状态
        self._skip = self._skip + (page - 1) * per_page
        self._limit = per_page
        self.rewind()
        items = []
        async for i in self:
            items.append(i)
        self._skip, self._limit = skip, limit  # 恢复当原状态
        return Pagination(self, page, per_page, total, items)

    def insert(self, objs, func=None, **kw):
        return wait([self._insert(data, func=func, **kw) for
                     data in _split(objs)])


class Aggregation:

    def __init__(self, document, pipeline=None, **kw):
        self.collection = document._collection
        self.pipeline = pipeline or []
        self.kw = kw or {}

    def __iter__(self):
        '''迭代返回所有值'''
        return self.collection.aggregate(self.pipeline, **self.kw)

    def paginate(self, page, per_page=20, error_out=True):
        """Returns `per_page` items from page `page`.  By default it will
        abort with 404 if no items were found and the page was larger than
        1.  This behavor can be disabled by setting `error_out` to `False`.

        Returns an :class:`Pagination` object.
        """
        if error_out and page < 1:
            abort(404)
        pipeline = self.pipeline.copy()
        pipeline.append({'$skip': (page - 1) * per_page})
        items = [i for i in
                 self.collection.aggregate(pipeline, **self.kw)]
        total = (page - 1) * per_page + len(items)
        items = items[:per_page]
        if not items and page != 1 and error_out:
            abort(404)

        # No need to count if we're on the first page and there are fewer
        # items than we expected.
        return Pagination(self, page, per_page, total, items)

    def project(self, *args, **kw):
        '''过滤字段'''
        for arg in args:
            kw[arg._name] = not arg._neg
        self.pipeline.append({'$project': kw})
        return self

    def match(self, kw):
        '''条件过滤'''
        if hasattr(kw, 'to_query'):
            kw = kw.to_query()
        self.pipeline.append({'$match': kw})
        return self

    def unwind(self, projection):
        '''打开列表字段'''
        if isinstance(projection, P):
            projection = projection._name
        if not projection.startswith('$'):
            projection = '$%s' % (projection)
        self.pipeline.append({'$unwind': projection})
        return self

    def group(self, *args):
        '''聚合字段'''
        _id = {}
        kw = {}
        for project in args:
            if isinstance(project, P):
                _id.update({project._name: "$%s" % (project._name)})
            else:
                kw.update(project.to_group())
        kw['_id'] = _id.popitem()[-1] if len(_id) == 1 else _id
        self.pipeline.append({'$group': kw})
        return self

    def sort(self, *args, **kw):
        '''对字段进行排序'''
        for arg in args:
            kw[arg._name] = -1 if arg._neg else 1
        self.pipeline.append({'$sort': kw})
        return self

    def skip(self, num):
        '''跳过记录'''
        self.pipeline.append({'$skip': num})
        return self

    def limit(self, num):
        '''限制输出记录'''
        self.pipeline.append({'$limit': num})
        return self

    def export(self, filename, sheetname='sheet1', range_="A1", columns=None,
               mapper=None):
        '''导出数据'''
        pass

    def scalar(self, *fields):
        for obj in self:
            obj = extract_dict(obj)
            yield [obj.get(k)for k in fields]
        '''
        for obj in self:
            row = []
            for field in fields:
                if '.' in field:
                    value = obj
                    for name in field.split('.'):
                        value = value[name]
                else:
                    value = obj[field]
                row.append(value)
            yield row
        '''


def extract_dict(d):
    for k in tuple(d.keys()):
        if isinstance(d.get(k), dict):
            for a, b in extract_dict(d.pop(k)).items():
                d['%s.%s' % (k, a)] = b
    return d
