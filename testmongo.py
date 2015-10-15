
from pymongo import MongoClient

client = MongoClient('amm-csr1', 27017)

for d in client.database_names():
    print d
    
db = client['raw_data']

print db

collection = db['miller_run']

print collection

#collection.insert({'name':'test',
                   #'value': 42})
       
for result in collection.find():
    print result
     

for d in client.database_names():
    print d
