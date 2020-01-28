from glemon import Document, P
from glemon.document import read_data
from orange import Path
from glemon.bulk import BulkWrite, enlist


class Test(Document):
    _projects = '_id,a,b'


text = '''A123B12C15
B123D12C16
C123D12E17
'''

with Path.tempfile(data=text, suffix='.csv')as p:
    options={
        'encoding':'gbk',
        'errors':'strict',
        'offsets':(0,4,7),
        'skip_header':False,
        'method':'insert',
        'upsert':False

    }
    Test.drop()
    Test.load_file(p,options,dry=True)