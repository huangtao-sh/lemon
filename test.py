from pymongo import MongoClient

c = MongoClient(host='mongodb://localhost/test')
db = c['test']
test = db['test']
'''
with c.start_session() as session:
    with session.start_transaction():
        test.insert_one({'a': 11, '_id': 6}, session=session)
        test.insert_one({'a': 11, '_id': 4}, session=session)
        session.commit_transaction()
        '''
for r in test.find():
    print(r)