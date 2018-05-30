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
        count = 20000
        data = [('abc-%d' % (x), x)for x in range(count)]
        run(Test1._insert(data, drop=True, projects=('a', 'b')))
        self.assertEqual(Test1.objects.count(), count)
        run(Test1._insert(data, drop=True, inorder=True))
        self.assertEqual(Test1.objects.count(), count)

    def test_second(self):
        self.assertEqual(1, 1)
