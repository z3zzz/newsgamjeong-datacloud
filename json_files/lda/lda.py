import pymongo
import json
from pprint import pprint

## 필수!! db, col 이름 변수 설정
db_from_name = 'xxx'
col_from_name = 'xxx'

db_to_name = 'final_data'
col_to_name = 'lda'

connection = pymongo.MongoClient("mongodb://localhost:27017/")

db = connection.get_database(db_from_name)
col = db.get_collection(col_from_name)

db_result = connection.get_database(db_to_name)
col_result = db_result.get_collection(col_to_name)

i = 0

for i in range(1,12):
    with open(f'month{i}.json', 'r') as f:
        data = json.load(f)
        ## 데이터 삽입 코드
        col_result.insert_one({
            "month": i,
            "data": data
        })



