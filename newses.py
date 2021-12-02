import pymongo
import json
from pprint import pprint

## 필수!! db, col 이름 변수 설정
db_from_name = 'final_data'
col_from_name = 'newses'

db_to_name = 'practice'
col_to_name = 'newses'

connection = pymongo.MongoClient("mongodb://localhost:27017/")

db = connection.get_database(db_from_name)
col = db.get_collection(col_from_name)

db_result = connection.get_database(db_to_name)
col_result = db_result.get_collection(col_to_name)

i = 0
for c in col.find({}):
    c.update({$set:{}})



'''

