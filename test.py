from pymongo import MongoClient
from asyncio import run, wait
from orange import Path, limit, timeit, R
from glemon import Document, P
from glemon.expr import updater
from pprint import pprint
import re
#print(P.abc.regex('abc').to_query())
#print((P.abc == 'hello').to_update())
#print(P.abc.unset().to_update())
from params.sjdr import sjdr

sjdr()