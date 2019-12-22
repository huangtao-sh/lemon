from pymongo import MongoClient
from asyncio import run, wait
from orange import Path, limit, timeit, R
from glemon import Document, P, enlist
from glemon.expr import updater
from pprint import pprint
import re
from glemon.load import LoadDocument
#print(P.abc.regex('abc').to_query())
#print((P.abc == 'hello').to_update())
#print(P.abc.unset().to_update())

path = Path(
    r'C:\Users\huangtao\OneDrive\工作\参数备份\运营管理2019-09\shendawei\ggbzb.del')


class Test(LoadDocument):
    _projects = enlist('_id,name,jc')
    load_options = {
        'file': {
            'encoding': 'gbk',
            #'columns': (0, 1)
        },
        'dupcheck': False,
        'mapper': {
            '_id': 0,
            'name': 2,
            'qt': 3,
        },
        'drop': True,
    }


Test.drop()


async def main():
    r = await Test.sync_load_file(path)
    print(r)


run(main())