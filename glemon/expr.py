# from functools import *
from orange import R
from collections import defaultdict


class _P(type):
    def __getitem__(self, name):
        if name == 'id':
            name = '_id'
        else:
            name = name.replace('__', '.')
            name = name.replace('.S.', '.$.')
        return P(name)

    __getattr__ = __getitem__


OPERATORS = {
    # update operator
    'set': '$set',
    'unset': '$unset',
    'inc': '$inc',
    'mul': '$mul',
    #'addToSet':'$addToSet',
    'pop': '$pop',
    'pushAll': '$pushAll',
    'pull': '$pull',
    'pullAll': '$pullAll',

    # project operator
    'slice': '$slice',
    'substr': '$substr',

    # query operator
    'in_': '$in',
    '_in': '$in',
    'nin': '$nin',
    'mod': '$mod',
    'all': '$all',
    'elemMatch': '$elemMatch',
    'size': '$size',

    # aggregation
    'push': '$push',
    'addToSet': '$addToSet',
    'min': '$min',
    'max': '$max',
    'sum': '$sum',
    'avg': '$avg',
    'first': '$first',
    'last': '$last',
}


class P(metaclass=_P):
    def __init__(self, name, neg=False):
        if name.startswith('-'):
            name, neg = name[1:], True
        self._name = name
        self._neg = neg

    def __neg__(self):
        self._neg = not self._neg
        return self

    def to_project(self):
        return {self._name: 0 if self._neg else 1}

    def __getattr__(self, name):
        if name in OPERATORS:
            return _Operator(self, OPERATORS.get(name))
        else:
            raise Exception('无此函数')

    def __eq__(self, value):
        return _Operator(self, None, value)

    def __lt__(self, value):
        return _Operator(self, '$lt', value)

    def __gt__(self, val):
        return _Operator(self, '$gt', val)

    def __le__(self, val):
        return _Operator(self, '$lte', val)

    def __ge__(self, val):
        return _Operator(self, '$gte', val)

    def __ne__(self, val):
        return _Operator(self, '$ne', val)

    def between(self, a, b):
        return (self >= a) & (self <= b)

    def to_group(self):
        return {'_id': f'${self._name}'}

    def to_order(self):
        return self._name, -1 if self._neg else 1

    def regex(self, *args):
        return _Operator(self, None)((R / args)._regex)

    contains = regex

    def startswith(self, val):
        return self.regex('^%s' % (val))

    def endswith(self, val):
        return self.regex('%s$' % (val))

    def icontains(self, val):
        return self.regex(val, 'i')

    def istartswith(self, val):
        return self.regex('^%s' % (val), 'i')

    def iendswith(self, val):
        return self.regex('%s$' % (val), 'i')

    def exists(self, val=True):
        return _Operator(self, '$exists', val)

    def unset(self, val=None):
        return _Operator(self, '$unset', val)

    def currentDate(self):
        return _Operator(self, '$currentDate')(True)

    def setOnInsert(self, val):
        return _Operator(self, '$setOnInsert', val)

    def in_(self, lst: list):
        '符合列表元素之一'
        return _Operator(self, '$in', lst)

    def reg_in(self, lst: list):
        '''
        query: P.abc.reg_in([r"abc",r"def"])
        正则表达式匹配'''
        return _Operator(self, '$in', [(R / r)._regex for r in lst])

    def count(self):
        'group: P.abc.count() '
        return _Operator(self, '$sum', 1)

    def sum(self, field=None):
        'group: P.abc.sum("$abc")'
        return _Operator(self, '$sum', field)


class Combin():
    def __init__(self, *items, op='$and', invert=False):
        self.op = op
        self.items = items
        self.invert = False

    def to_query(self):
        if self.invert and self.op == '$or':
            self.op = '$nor'
        return {self.op: [item.to_query() for item in self.items]}

    def _oper(self, other, op):
        if self.op == op and not self.invert:
            return Combin(*self.items, other, op=op)
        else:
            return Combin(self, other, op=op)

    def __and__(self, new):
        return self._oper(new, '$and')

    def __or__(self, new):
        return self._oper(new, '$or')

    def __invert__(self):
        self.invert = not self.invert
        return self


def And(*items):
    '组合多个查询条件，用 and 连接'
    '全部为真，返回真'
    return Combin(*items, op="$and")


def Or(*items):
    '组合多个查询条件，用 or 连接'
    '任一为真，返回真'
    return Combin(*items, op='$or')


def Nor(*items):
    '组合多个查询条件，用 nor 连接'
    '全部为假，返回真'
    return Combin(*items, op='$nor')


class _Operator():
    def __init__(self, project, operator=None, args=None, kw=None):
        self.project = project._name
        self.operator = operator
        self.invert = False
        self.kw = kw or {}
        self.args = args

    def __invert__(self):
        self.invert = not self.invert
        return self

    def __call__(self, args, extra=None):
        self.args = args
        return self

    def __and__(self, other):
        if isinstance(other, _Operator):
            if self.project == other.project:
                self.kw.update({other.operator: other.args})
                return self
            else:
                return Combin(self, other, op='$and')
        else:
            return other.__and__(self)

    def __or__(self, other):
        if isinstance(other, _Operator):
            return Combin(self, other, op='$or')
        else:
            return other.__or__(self)

    def to_query(self):
        kw = {}
        if self.kw:
            kw.update(self.kw)
        if self.operator:
            kw.update({self.operator: self.args})
        else:
            kw = self.args
        if self.invert:
            # regex 不支持使用 $ne，故使用 $not
            if self.operator or hasattr(self.args, 'pattern'):
                kw = {'$not': kw}
            else:
                kw = {'$ne': self.args}
        return {self.project: kw}

    to_project = to_query

    def to_update(self):
        return {self.operator or '$set': {self.project: self.args}}

    def to_group(self):
        return {self.project: {self.operator: self.args or f"${self.project}"}}

    def _update(self, updater):
        op = self.operator or '$set'
        updater.setdefault(op, {})
        updater[op].update({self.project: self.args})


def updater(*args, **kw):
    result = {}
    for r in args:
        r._update(result)
    for k, v in kw.items():
        if not k.startswith('$'):
            v = {k: v}
            k = '$set'
        result.setdefault(k, {})
        result[k].update(v)
    return result
