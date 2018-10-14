from glemon import Document, P, FileImported
import unittest
from orange import Path


class TestLoadFile(Document):
    _projects = 'a', 'b', 'c', 'd'
    load_options = {
        'dupcheck': False,
        'converter': {
            int: 0,
            str.strip: 2,
        }
    }



text = '''1,"huangtao","黄涛  ",35
2,"lisi","李四   ",36
3,"wangwu",,412902197909022052'''


class TestLoad(unittest.TestCase):
    def setUp(self):
        TestLoadFile.drop()

    def testEncoding(self):
        options = {
            'drop': True
        }
        for code in ('utf8', 'gbk', 'cp936', 'gb2312'):
            with Path.tempfile(text.encode(code), suffix='.csv')as f:
                r = TestLoadFile.loadfile(f, options)
                self.assertEqual(r.inserted_count, 3)
            self.assertEqual(TestLoadFile.objects.count(), 3)
            r = TestLoadFile.objects.filter(a=1).first()
            self.assertEqual(r.c, '黄涛')

    def testEncoding2(self):
        options = {
            'drop': True
        }
        for code in ('utf8', 'gbk', 'cp936', 'gb2312'):
            options['encoding'] = code
            with Path.tempfile(text.encode(code), suffix='.csv')as f:
                r = TestLoadFile.loadfile(f, options)
                self.assertEqual(r.inserted_count, 3)
            self.assertEqual(TestLoadFile.objects.count(), 3)
            r = TestLoadFile.objects.filter(a=1).first()
            self.assertEqual(r.c, '黄涛')

    def testDupCheck(self):
        options = {
            'dupcheck': True
        }
        with Path.tempfile(text, suffix='.del')as f:
            r = TestLoadFile.loadfile(f, options)
            self.assertEqual(r.inserted_count, 3)
            with self.assertRaises(FileImported):
                r = TestLoadFile.loadfile(f, options)

    def testInsert2(self):
        options = {
            'fields': 'a,b,,c',
        }
        with Path.tempfile(text, suffix='.del')as f:
            r = TestLoadFile.loadfile(f, options)
            self.assertEqual(r.inserted_count, 3)
        self.assertEqual(TestLoadFile.objects.count(), 3)
        r = TestLoadFile.objects.filter(a=1).first()
        self.assertEqual(r.b, 'huangtao')
        self.assertEqual(r.c, '35')
        self.assertEqual(r.d, None)

    def testInsert(self):
        with Path.tempfile(text, suffix='.csv')as f:
            r = TestLoadFile.loadfile(f)
            self.assertEqual(r.inserted_count, 3)
        self.assertEqual(TestLoadFile.objects.count(), 3)
        r = TestLoadFile.objects.filter(a=1).first()
        self.assertEqual(r.b, 'huangtao')

        with Path.tempfile(text, suffix='.csv')as f:
            options = {'method': 'replace', 'keys': 'a'}
            r = TestLoadFile.loadfile(f, options)
            self.assertEqual(r.upserted_count, 3)
        r = TestLoadFile.objects.filter(a=1).first()
        self.assertEqual(r.b, 'huangtao')

        with Path.tempfile(text, suffix='.csv')as f:
            options = {'method': 'update', 'keys': 'a'}
            r = TestLoadFile.loadfile(f, options)
            self.assertEqual(r.upserted_count, 3)
        r = TestLoadFile.objects.filter(a=1).first()
        self.assertEqual(r.b, 'huangtao')
