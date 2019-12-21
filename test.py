from pymongo import MongoClient
from asyncio import run, wait
from orange import Path, limit, timeit, R
from glemon import Document, P, enlist
from glemon.expr import updater
from pprint import pprint
import re
#print(P.abc.regex('abc').to_query())
#print((P.abc == 'hello').to_update())
#print(P.abc.unset().to_update())


class Test(Document):
    _projects = enlist('_id,b,c')


Test.drop()

c = Test.get_collection()
c.insert_one({'_id': 'a',"a":"b"})
Test(_id='test1',a='hello').save()

for r in Test.objects:
    print(r)
