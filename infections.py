import pymongo
import json
from pprint import pprint

## 필수!! db, col 이름 변수 설정
db_from_name = 'final_data'
col_from_name = 'infections'

db_to_name = 'final_data'
col_to_name = 'infections'

connection = pymongo.MongoClient("mongodb://localhost:27017/")
db = connection.get_database(db_from_name)
col = db.get_collection(col_from_name)


result = []
sums = 0
for c in col.find({}):
    temp = {}
    c["date"] = c["date"].replace('-','.')
    c["corona"] = c["corona"].replace(',','')
    temp["date"] = c["date"]
    temp["corona"] = c["corona"]
    temp["new"] = "yes"
    #sums += c["corona"]

    result.append(temp)

db_result = connection.get_database(db_to_name)
col_result = db_result.get_collection(col_to_name)

col_result.insert_many(result)


