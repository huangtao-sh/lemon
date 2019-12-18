from pymongo import MongoClient
from asyncio import run, wait
from orange import Path, limit, timeit
from glemon import Document


class Test(Document):
    _projects = '_id,b,c'
    pass


def read():
    for r in range(10):
        yield [r, f'b-{r}', f'c-{r}']


blk = Test.bulkwrite(Test, read())

Test.drop()
r = run(blk.sync_execute())
print(r)
for r in Test.objects:
    print(r)