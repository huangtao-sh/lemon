from glemon import Document, P, FileImported
import unittest
from orange import Path
from glemon.document import enlist


class TestLoadDoc(Document):
    _projects = enlist('_id,name,age')
    load_options = {
        'header': {
            '编号': 'no',
            '姓名': ('name', str.strip),
            '年龄': ('age', int)
        }
    }


doctext = '''
序号,编号,姓名,年龄
1,001,"张三    ",23
2,002,"王五    ",27
'''


class TestLoad2(unittest.TestCase):
    def setUp(self):
        TestLoadDoc.drop()

    tearDown = setUp

    def testLoad(self):
        with Path.tempfile(doctext, suffix='.csv') as f:
            r = TestLoadDoc.loadfile(f)
            self.assertEqual(r.inserted_count, 2)
        obj = TestLoadDoc.objects.filter(no='001').first()
        obj = dict(obj)
        obj.pop('_id')
        self.assertDictEqual(obj, {'no': '001', 'name': '张三', 'age': 23})


class TestLoadFile(Document):
    _projects = 'a', 'b', 'c', 'd'
    load_options = {
        'dupcheck': False,
        'converter': {
            'a': int,
            'b,c,d': str.strip,
        }
    }


text = '''1,"huangtao","黄涛  ",35
2,"lisi","李四   ",36
3,"wangwu",,412902197909022052'''


class TestLoad(unittest.TestCase):
    def setUp(self):
        TestLoadFile.drop()

    def testEncoding(self):
        options = {'drop': True}
        for code in ('utf8', 'gbk', 'cp936', 'gb2312'):
            with Path.tempfile(text.encode(code), suffix='.csv') as f:
                r = TestLoadFile.loadfile(f, options)
                self.assertEqual(r.inserted_count, 3)
            self.assertEqual(TestLoadFile.objects.count(), 3)
            r = TestLoadFile.objects.filter(a=1).first()
            self.assertEqual(r.c, '黄涛')

    def testEncoding2(self):
        options = {'drop': True}
        for code in ('utf8', 'gbk', 'cp936', 'gb2312'):
            options['encoding'] = code
            with Path.tempfile(text.encode(code), suffix='.csv') as f:
                r = TestLoadFile.loadfile(f, options)
                self.assertEqual(TestLoadFile.objects.count(), 3)
            self.assertEqual(TestLoadFile.objects.count(), 3)
            r = TestLoadFile.objects.filter(a=1).first()
            self.assertEqual(r.c, '黄涛')

    def testDupCheck(self):
        options = {'dupcheck': True}
        with Path.tempfile(text, suffix='.del') as f:
            r = TestLoadFile.loadfile(f, options)
            self.assertEqual(r.inserted_count, 3)
            with self.assertRaises(FileImported):
                r = TestLoadFile.loadfile(f, options)

    def testInsert2(self):
        options = {
            'fields': 'a,b,,c',
        }
        with Path.tempfile(text, suffix='.del') as f:
            r = TestLoadFile.loadfile(f, options)
            self.assertEqual(r.inserted_count, 3)
        self.assertEqual(TestLoadFile.objects.count(), 3)
        r = TestLoadFile.objects.filter(a=1).first()
        self.assertEqual(r.b, 'huangtao')
        self.assertEqual(r.c, '35')
        self.assertEqual(r.d, None)

    def testInsert(self):
        with Path.tempfile(text, suffix='.csv') as f:
            r = TestLoadFile.loadfile(f)
            self.assertEqual(r.inserted_count, 3)
        self.assertEqual(TestLoadFile.objects.count(), 3)
        r = TestLoadFile.objects.filter(a=1).first()
        self.assertEqual(r.b, 'huangtao')

        with Path.tempfile(text, suffix='.csv') as f:
            options = {'method': 'replace', 'keys': 'a'}
            r = TestLoadFile.loadfile(f, options)
            self.assertEqual(r.modified_count, 3)
        r = TestLoadFile.objects.filter(a=1).first()
        self.assertEqual(r.b, 'huangtao')

        with Path.tempfile(text, suffix='.csv') as f:
            options = {'method': 'update', 'keys': 'a'}
            r = TestLoadFile.loadfile(f, options)
        r = TestLoadFile.objects.filter(a=1).first()
        self.assertEqual(r.b, 'huangtao')

    def testEnlist(self):
        fields1 = '_id,name,age'
        fields2 = fields1.split(',')
        fields3 = tuple(fields2)
        for fields in (fields1, fields2, fields3):
            self.assertListEqual(fields2, list(enlist(fields)))

    def testLoad(self):
        def data():
            for r in range(200000):
                yield (r, f'abc{r}', 25)

        TestLoadDoc.bulk_write(data(), drop=True, method='insert')
        self.assertEqual(TestLoadDoc.objects.count(), 200000)
        TestLoadDoc.objects.filter(P._id == 0).update_one(name='abc')
        self.assertEqual(TestLoadDoc.objects.get(0).name, 'abc')
        count = 30

        def data2():
            for r in range(count):
                yield (r, f'abcd{r}', 27)

        TestLoadDoc.bulk_write(data2(),
                               drop=True,
                               keys='_id',
                               fields='_id,name,age',
                               method='replace')
        self.assertEqual(
            TestLoadDoc.objects.filter(P.age == 27).count(), count)
