import unittest
from lemon import *
from orange.coroutine import *

class Test(Document):
    _projects='a b'.split()

    #_textfmt='{self.a}\t{self.b}'


class TestLemon(unittest.TestCase):
    def setUp(self):
        Test.drop()

    def tearDown(self):
        Test.drop()

    def test_asave(self):
        async def _():
            a=Test(id=10,a=20,b=30)
            await a.asave()
            t=await Test.abjects.first()
            self.assertEqual(a.a,t.a)
            t.b=300
            self.assertTrue(t._modified)
            await t.asave()
            self.assertFalse(t._modified)
            a=await Test.abjects.first()
            self.assertEqual(a.b,t.b)
        run(_())
        
    def test_insert_one(self):
        a={'a':1,'b':2}
        d={'a':1,'b':2}
        Test.insert_one(d)
        b=Test.objects.first()
        self.assertEqual(d,b)
        Test.insert_one(a)
        self.assertListEqual(Test.objects.distinct('a'),[1])
        self.assertEqual(Test.objects.count(),2)
        Test.objects(P.a==3).upsert(b=4)
        b=Test.objects(P.a==3).first()
        self.assertEqual(b.b,4)
        Test.objects.update(P.b.inc(10))
        b=Test.objects(P.a==3).first()
        self.assertEqual(b.b,14)

        Test.objects(P.a==3).delete()
        a=Test.objects(P.a==3).first()
        self.assertIsNone(a)
        Test.objects(P.a==1).delete_one()
        self.assertEqual(Test.objects(P.a==1).count(),1)
        
    def test_asyncio(self):
        async def _():
            a={'a':1,'b':2}
            await Test.ansert_one(a)
            b=await Test.abjects.first()
            self.assertDictEqual(a,b)
            await Test.abjects(P.a==1).delete_one()
            self.assertEqual(await Test.abjects.count(),0)

            await Test.abjects(P.a==1).upsert(b=10)
            a=await Test.abjects(P.a==1).first()
            self.assertEqual(a.b,10)
            await Test.abjects(P.a==1).update(P.b.inc(10))
            a=await Test.abjects(P.a==1).first()
            self.assertEqual(a.b,20)
            d={'a':15,'b':20}
            await Test.abjects.insert([d])
            a=await Test.abjects(P.a==15).first()
            self.assertEqual(a.b,20)
            
        run(_())

    def test_batch(self):
        func=lambda x: {'a':x,'b':x+100}
        b=Test.objects.insert(range(1024),func=func)
        a=Test.objects.count()
        self.assertEqual(a,1024)
        self.assertEqual(a,b)

    def test_asyncio_batch(self):
        async def _():
            func=lambda x: {'a':x,'b':x+100}
            await Test.abjects.insert(range(1024),func=func)
            a=await Test.abjects.count()
            self.assertEqual(a,1024)
            s=await Test.abjects.distinct('a')
            d=[]
            async for x in Test.abjects.scalar('a'):
                d.append(x)
            self.assertSetEqual(set(s),set(range(1024)))
            self.assertSetEqual(set(d),set(range(1024)))
        run(_())

    def test_asyncio_batch2(self):
        async def _():
            sl=1024
            func=lambda x: {'a':x,'b':x+100}
            b=await Test.abjects.insert(range(sl),func=func)
            a=await Test.abjects.count()
            self.assertEqual(a,sl)
        run(_())

    def test_save(self):
        t=Test(a=10,b=100)
        self.assertEqual(t._modified,True)
        t.save()
        a=Test.objects.first()
        self.assertEqual(t._modified,False)
        self.assertEqual(t.a,a.a)
        a.b=20000
        self.assertEqual(a._modified,True)
        a.save()
        self.assertEqual(a._modified,False)
        t=Test.objects.first()
        self.assertEqual(a.b,t.b)
        t=Test(id=10,a=40,b=60)
        self.assertTrue(t._modified)
        t.save()
        b=Test.objects.get(10)
        self.assertEqual(t.a,b.a)
