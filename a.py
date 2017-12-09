from glemon import *
import json


class Test(Document):
    _projects = 'a', 'b'


Test.drop()
a = Test.objects
objects = [{'a': i, 'b': i+200}for i in range(1, 104)]
a.insert(objects)

a(P.a > 10).limit(34)
for i in a.paginate(per_page=20):
    print(i.a, i.b)

print('-'*30)
_id = a.cache()


k = load_query(_id)
for i in k.paginate(page=2):
    print(i.a, i.b)
