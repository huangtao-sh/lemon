from lemon.document import *
from orange.coroutine import *

class Test(Document):
    _projects=('a','b')

Test.drop()

def main(sl=100000):
    async def _():
        await Test.abjects.insert(range(sl),func=lambda x:{'a':x,'b':x+100})

    run(_())

def b(sl=100000):
    Test.objects.insert(range(sl),func=lambda x:{'a':x,'b':x+100})

    
import profile
profile.run("main()")
