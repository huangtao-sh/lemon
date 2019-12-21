from pymongo import MongoClient
from asyncio import run, wait
from orange import Path, limit, timeit, R
from glemon import Document, P
from glemon.expr import updater
from pprint import pprint
import re
#print(P.abc.regex('abc').to_query())
#print((P.abc == 'hello').to_update())
#print(P.abc.unset().to_update())


class Test(Document):
    _projects = 'a,b,c'


up = updater(P.abc.unset(),
             P.test.unset(),
             P.createdate.currentDate(),
             P.abcd.setOnInsert('abc'),
             name='hunter')

Test.find(P.name=='hunter').update(P.createdate.unset(),hello='world')

for r in Test.objects:
    print(r)