from pymongo import MongoClient
from asyncio import run, wait
from orange import Path, limit, timeit, R
from glemon import Document, P, enlist
from glemon.expr import updater
from pprint import pprint
import re
from glemon.load import LoadDocument
from orange.xlsx import Header
#print(P.abc.regex('abc').to_query())
#print((P.abc == 'hello').to_update())
#print(P.abc.unset().to_update())


class Test(LoadDocument):
    _projects = enlist('a,b,c')


def create():
    for i in range(300, 350):
        yield [f'a-{i}', f'b-{i}', f'c-{i}']


Test.bulk_write(create(), drop=True)
t = Test.aggregate()
t.match(P.a > 'a-340')
t.project(-P.id, P.a, P.b)
t.export('~/test.xlsx',
         projects=['a', 'b'],
         columns=[Header('A1'), Header('B1')],
         force=True)
