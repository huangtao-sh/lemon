# 项目：数据库模型
# 模块：测试数据库
# 作者：黄涛
# License:GPL
# Email:huangtao.sh@icloud.com
# 创建：2018-05-30 21:13

from glemon.doc import NewDocument as Document
import unittest
from orange.coroutine import run


class Test1(Document):
    _projects = '_id', 'age'


class TestDoc(unittest.TestCase):
    def test_insert(self):
        count = 200
        data = [('abc-%d' % (x), x)for x in range(count)]
        run(Test1._insert(data, drop=True, projects=('a', 'b')))
        self.assertEqual(Test1.objects.count(), count)
        run(Test1._insert(data, drop=True, inorder=True))
        self.assertEqual(Test1.objects.count(), count)

    def test_second(self):
        self.assertEqual(1, 1)

    def test_uodate(self):
        count = 200
        data = [('abc-%d' % (x), x)for x in range(count)]
        Test1.drop()
        run(Test1._update(data, upsert=True))
        self.assertEqual(Test1.objects.count(), count)

    def test_bulkwrite(self):
        count = 10
        row = [(x, f'name{x}') for x in range(count)]
        for method in ('insert', 'replace', 'update'):
            Test1.bulk_write(row, method=method)
            self.assertEqual(Test1.objects.count(), count)
            Test1.drop()

    def test_bulkwrite2(self):
        class Test3(Document):
            _projects = '_id', 'name', 'age'
        data = [['a', 'b', 'c', 'd']]
        for method in ('insert', 'replace', 'update'):
            Test3.bulk_write(data, method=method, keys=('_id', 'name'))
            self.assertEqual(Test3.objects.count(), 1)
            obj = Test3.objects.first()
            self.assertEqual(obj.id, 'a')
            self.assertEqual(obj.name, 'b')
            self.assertEqual(obj.age, 'c')
        Test3.drop()
        for method in ('insert', 'replace', 'update'):
            Test3.bulk_write(data, method=method, keys=('_id', 'name'),
                             fields='_id,,name,age')
            self.assertEqual(Test3.objects.count(), 1)
            obj = Test3.objects.first()
            self.assertEqual(obj.id, 'a')
            self.assertEqual(obj.name, 'c')
            self.assertEqual(obj.age, 'd')
        Test3.drop()
        for method in ('insert', 'replace', 'update'):
            Test3.bulk_write(data, method=method, keys=('_id',),
                             mapper={'_id':0,'name':2,'age':3})
            self.assertEqual(Test3.objects.count(), 1)
            obj = Test3.objects.first()
            self.assertEqual(obj.id, 'a')
            self.assertEqual(obj.name, 'c')
            self.assertEqual(obj.age, 'd')
        Test3.drop()
