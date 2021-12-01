import pymongo
import json
from pprint import pprint

## 필수!! db, col 이름 변수 설정
db_name = 'final_data'
col_name = 'newses'

connection = pymongo.MongoClient("mongodb://localhost:27017/")
db = connection.get_database(db_name)
col = db.get_collection(col_name)

print(col.count_documents({"date":"2021.03.01"}))


