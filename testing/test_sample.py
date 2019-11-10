import unittest
from glemon import Document, P
from orange.coroutine import run
from orange import Path, tempfile
import glemon.paginate
import glemon.loadfile
import glemon.loadcheck
import glemon.query
import glemon.cachedquery
import glemon.config
import glemon.shadow
import glemon.expr
from glemon.document import Descriptor


class Test(Document):
    _projects = 'a', 'b'
    _load_mapper = {'a': 'hello', 'b': 'world'}
    b_dtl = Descriptor('b', {'0': 'hello', '1': 'world'})

    # _textfmt='{self.a}\t{self.b}'


class TestShadow(unittest.TestCase):

    def test_shadow(self):
        from glemon.shadow import Shadow
        a = {'user': 'zhangsan', 'passwd': '123456', 'abc': 'work'}
        Shadow.write('shadow_test', a)
        self.assertDictEqual(a, Shadow.read('shadow_test'))
        Shadow.write('shadow_test', None)
        self.assertEqual(None, Shadow.read('shadow_test'))


class TestLemon(unittest.TestCase):

    def setUp(self):
        Test.drop()

    def tearDown(self):
        Test.drop()

    def _test_asave(self):
        async def _():
            a = Test(id=10, a=20, b=30)
            await a.asave()
            t = await Test.abjects.first()
            self.assertEqual(a.a, t.a)
            t.b = 300
            self.assertTrue(t._modified)
            await t.asave()
            self.assertFalse(t._modified)
            a = await Test.abjects.first()
            self.assertEqual(a.b, t.b)
        run(_())

    def _test_insert_one(self):
        a = {'a': 1, 'b': 2}
        d = {'a': 1, 'b': 2}
        Test.insert_one(d)
        b = Test.objects.first()
        self.assertEqual(d, b)
        Test.insert_one(a)
        self.assertListEqual(Test.objects.distinct('a'), [1])
        self.assertEqual(Test.objects.count(), 2)
        Test.objects(P.a == 3).upsert(b=4)
        b = Test.objects(P.a == 3).first()
        self.assertEqual(b.b, 4)
        Test.objects.update(P.b.inc(10))
        b = Test.objects(P.a == 3).first()
        self.assertEqual(b.b, 14)

        Test.objects(P.a == 3).delete()
        a = Test.objects(P.a == 3).first()
        self.assertIsNone(a)
        Test.objects(P.a == 1).delete_one()
        self.assertEqual(Test.objects(P.a == 1).count(), 1)

    def _test_asyncio(self):
        async def _():
            a = {'a': 1, 'b': 2}
            await Test.ansert_one(a)
            b = await Test.abjects.first()
            self.assertDictEqual(a, b)
            await Test.abjects(P.a == 1).delete_one()
            self.assertEqual(await Test.abjects.count(), 0)

            await Test.abjects(P.a == 1).upsert(b=10)
            a = await Test.abjects(P.a == 1).first()
            self.assertEqual(a.b, 10)
            await Test.abjects(P.a == 1).update(P.b.inc(10))
            a = await Test.abjects(P.a == 1).first()
            self.assertEqual(a.b, 20)
            d = {'a': 15, 'b': 20}
            await Test.abjects.insert([d])
            a = await Test.abjects(P.a == 15).first()
            self.assertEqual(a.b, 20)

        run(_())
    '''
    def test_batch(self):
        Test.drop()
        def func(x): return {'a': x, 'b': x + 100}
        b = Test.objects.insert(range(1024), func=func)
        a = Test.objects.count()
        self.assertEqual(a, 1024)
        self.assertEqual(a, b)
    '''

    def _test_asyncio_batch(self):
        async def _():
            def func(x): return {'a': x, 'b': x + 100}
            await Test.abjects.insert(range(1024), func=func)
            a = await Test.abjects.count()
            self.assertEqual(a, 1024)
            s = await Test.abjects.distinct('a')
            d = []
            async for x in Test.abjects.scalar('a'):
                d.append(x)
            self.assertSetEqual(set(s), set(range(1024)))
            self.assertSetEqual(set(d), set(range(1024)))
        run(_())

    def _test_asyncio_batch2(self):
        async def _():
            sl = 1024

            def func(x): return {'a': x, 'b': x + 100}
            await Test.abjects.insert(range(sl), func=func)
            a = await Test.abjects.count()
            self.assertEqual(a, sl)
        run(_())

    def _test_save(self):
        t = Test(a=10, b=100)
        self.assertEqual(t._modified, True)
        t.save()
        a = Test.objects.first()
        self.assertEqual(t._modified, False)
        self.assertEqual(t.a, a.a)
        a.b = 20000
        self.assertEqual(a._modified, True)
        a.save()
        self.assertEqual(a._modified, False)
        t = Test.objects.first()
        self.assertEqual(a.b, t.b)
        t = Test(id=10, a=40, b=60)
        self.assertTrue(t._modified)
        t.save()
        b = Test.objects.get(10)
        self.assertEqual(t.a, b.a)
    '''
    def test_descriptor(self):
        Test.drop()
        Test(a='0', b='0').save()
        obj = Test.objects.first()
        self.assertEqual(obj.b_dtl, '0-hello')
        Test(a='1', b='1').save()
    '''
    def test_dupcheck(self):
        from glemon.loadcheck import dup_check, FileImported, LoadFile
        path = Path('test.txt')
        path.lines = ['a', 'b']
        try:
            LoadFile.drop()
            checker = dup_check(path, 'test')
            checker.done()
            self.assertRaises(FileImported, dup_check, path, 'test')
        finally:
            path.unlink()

    def testShadow(self):
        from glemon.shadow import profile, Shadow
        ver = '2018-09'
        profile.test_version = ver
        ver = Shadow.read('test_version')
        self.assertEqual(ver, profile.test_version)
