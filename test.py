from glemon.load import LoadDocument as Document,P
from pymongo import InsertOne
from glemon.bulk import BulkWrite, enlist


class Test(Document):
    _projects = '_id,a,b'


def create_data(count):
    for i in range(count):
        yield InsertOne(
            dict(zip(enlist(Test._projects), [f'a-{i}', f'b-{i}', f'c-{i}'])))

Test.drop()
Test.bulk_write(requests=create_data(100))

r=Test.find().first()
print(r)
print(r.update(name='huangtao'))
print(Test.find().first())