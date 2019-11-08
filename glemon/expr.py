# from functools import *
from orange import R


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
    'currentDate': '$currentDate',
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
        return _Operator(self, None)(value)

    def __lt__(self, value):
        return _Operator(self, '$lt')(value)

    def __gt__(self, val):
        return _Operator(self, '$gt')(val)

    def __le__(self, val):
        return _Operator(self, '$lte')(val)

    def __ge__(self, val):
        return _Operator(self, '$gte')(val)

    def __ne__(self, val):
        return _Operator(self, '$ne')(val)

    def between(self, a, b):
        return (self >= a) & (self <= b)

    def to_group(self):
        return {'_id': '$%s' % (self._name)}

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
        return _Operator(self, '$exists')(val)


class Combin():
    def __init__(self, *items, op='$and'):
        self.op = op
        self.items = list(items)
        self.invert = False

    def to_query(self):
        if self.invert and self.op == '$or':
            self.op = '$nor'
        return {self.op: [item.to_query() for item in self.items]}

    def _oper(self, other, op):
        if self.op == op:
            self.items.append(other)
            return self
        else:
            return Combin(self, other, op=op)

    def __and__(self, new):
        return self._oper(new, '$and')

    def __or__(self, new):
        return self._oper(new, '$or')

    def __invert__(self):
        self.invert = not self.invert
        return self


class _Operator():
    def __init__(self, project, operator=None, kw=None):
        self.project = project._name
        self.operator = operator
        self.invert = False
        self.kw = kw or {}

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
        return {self.operator: {self.project: self.args}}

    def to_group(self):
        return {self.project: {self.operator: self.args}}
