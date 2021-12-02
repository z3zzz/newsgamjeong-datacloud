import pymongo
import json
from pprint import pprint

## 필수!! db, col 이름 변수 설정
db_from_name = 'final_data'
col_from_name = 'line_graph'

db_to_name = 'final_data'
col_to_name = 'line_graph'

connection = pymongo.MongoClient("mongodb://localhost:27017/")
db = connection.get_database(db_from_name)
col = db.get_collection(col_from_name)


result = []
sums = 0
for c in col.find({}):
    temp = {}
    temp["date"] = c["날짜"]
    temp["social"] = c["사회"]
    temp["economics"] = c["경제"]
    temp["life"] = c["생활/문화"]
    temp["politics"] = c["정치"]
    temp["month"] = c["날짜"][5:7]
    temp["new"] = "yes"

    result.append(temp)

'''

자료 삽입용 -
db_result = connection.get_database(db_to_name)
col_result = db_result.get_collection(col_to_name)

col_result.insert_many(result)
'''

