from pymongo import MongoClient
from asyncio import run, wait
from orange import Path, limit, timeit
'''
c = MongoClient(host='mongodb://localhost/test')
db = c['test']
test = db['test']
with c.start_session() as session:
    with session.start_transaction():
        test.insert_one({'a': 11, '_id': 6}, session=session)
        test.insert_one({'a': 11, '_id': 4}, session=session)
        session.commit_transaction()
for r in test.find():
    print(r)

'''
from glemon.data import BulkWrite, Document, Data, BulkResult


class Test(Document):
    _projects = '_id', 'jgm', 'bz', 'hm'


path = Path(r'C:/Users/huangtao/OneDrive/工作/参数备份/运营管理2019-09')
path = path.find('fhnbhzz.*')
data = path.iter_csv(encoding='gbk', errors='ignore', columns=(0, 1, 2, 3, 4))

bulk = BulkWrite(data, Test._projects, Test)


@timeit
def test1():
    return bulk.execute()


@timeit
def test2():
    run(bulk.sync_execute())


'''
print(Test.objects.count())

data = path.iter_csv(encoding='gbk', errors='ignore', columns=(0, 1, 2, 3, 4))
bulk = BulkWrite(data, Test._projects, Test)
Test.drop()
test2()
print(Test.objects.count())
exit()
'''
#Test.drop()
data = path.iter_csv(encoding='gbk', errors='ignore', columns=(0, 1, 2, 3, 4))
bulk = BulkWrite(data, Test._projects, Test, method='update')
#Test.drop()
print(test1())
print(Test.objects.count())
