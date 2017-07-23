import unittest
from lemon.document import *

class Test(Document):
    _projects='a b'.split()
    
async def test(self):
    await Test.insert({'a':1,'b':2})

class TestLemon:
    def __init__(self,test):
        self.test=test
        async def main():
            for t in [getattr(self,x)for x in dir(self) if x.startswith('test_')]:
                try:
                    await t()
                finally:
                    await Test.drop()
        start(main())

    def equal(self,*args):
        self.test.assertEqual(*args)
        
    async def test_insert(self):
        obj=await Test.insert({'a':1,'b':2})
        await sleep(0.1)
        a=await Test.objects.get(str(obj.inserted_id))       # 测试 get
        self.test.assertEqual(a.a,1)
        a=await Test.objects.get_or_404(str(obj.inserted_id)) # 测试get_or_404
        self.test.assertEqual(a.a,1)
        a=await Test.objects.first()              # 测试 first
        self.test.assertEqual(a.a,1)
        a=await Test.objects.first_or_404()      # 测试 first_or_404
        self.test.assertEqual(a.a,1)
        count=await Test.objects.count()         # 测试 count
        self.test.assertEqual(count,1)
        
    async def test_search(self):
        await batch(lambda x:Test.insert({'a':x,'b':x+10}),range(10))
        await batch(lambda x:Test.insert({'a':x,'b':x+10}),range(100,110))
        count=await Test.objects.count()
        self.test.assertEqual(count,20)
        count=await Test.objects(P.a>10).count()
        self.test.assertEqual(count,10)
        await Test.insert({'a':'HuangTao','b':'test'})
        a=await Test.objects(P.a.startswith('Huang')).first()
        self.test.assertEqual(a.b,'test')
        a=await Test.objects(P.a.endswith('Tao')).first()
        self.test.assertEqual(a.b,'test')
        a=await Test.objects(P.a.istartswith('huang')).first()
        self.test.assertEqual(a.b,'test')
        a=await Test.objects(P.a.contains('uang')).first()
        self.test.assertEqual(a.b,'test')
        a=await Test.objects(P.a.icontains('Uang')).first()
        self.test.assertEqual(a.b,'test')
        a=await Test.objects(P.a.iendswith('Gtao')).first()
        self.equal(a.b,'test')
        
    async def test_update(self):
        await batch(lambda x:Test.insert({'a':x,'b':x+10}),range(10))
        await Test.objects(P.a<10).update(P.b.inc(1))
        x=[]
        async for i in Test.objects(P.a<10):
            x.append(i.b)
        self.test.assertListEqual(sorted(x),list(sorted(range(11,21))))
        await Test.objects(P.a=='huangtao').upsert_one(b='lisi')
        a=await Test.objects(P.a=='huangtao').first()
        self.equal(a.b,'lisi')

        await Test.objects(P.a=='huangtao').update_one(b='machao')
        a=await Test.objects(P.a=='huangtao').first()
        self.equal(a.b,'machao')
        

class TestSample(unittest.TestCase):
    def test_lemon(self):
        #TestLemon(self)
        pass

