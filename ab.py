from glemon import Document


class Test(Document):
    _projects = ('_id', 'name')
    load_options = {'dupcheck': True,
    'fields':'_id,name',
    'keys':'_id',
    'method':'replace'
    }


Test.drop()
Test.loadfile('a.csv')
for obj in Test.objects:
    print(obj.id, obj.name)
