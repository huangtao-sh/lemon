from glemon.load import LoadDocument as Document
from glemon.bulk import BulkWrite


class Test(Document):
    _projects = '_id,a,b'


def create_data(count):
    for i in range(count):
        yield [f'a-{i}', f'b-{i}', f'c-{i}']


blk = BulkWrite(Test, data=create_data(10),keys='b',method='replace',upsert=False)
for r in blk:
    print(r)