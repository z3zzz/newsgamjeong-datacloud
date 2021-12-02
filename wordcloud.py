import pymongo
import json
from pprint import pprint

## 필수!! db, col 이름 변수 설정
db_from_name = 'practice'
col_from_name = 'wordcloud'

db_to_name = 'final_data'
col_to_name = 'wordcloud'

connection = pymongo.MongoClient("mongodb://localhost:27017/")

db = connection.get_database(db_from_name)
col = db.get_collection(col_from_name)

db_result = connection.get_database(db_to_name)
col_result = db_result.get_collection(col_to_name)

i = 0
c = col.find_one({})

result = []
for key in c.keys():
    if key == "total":
        continue

    month, keyword = key.split('_')
    temp = {}
    temp["month"] = month
    temp["keyword"] = keyword
    temp["wordcloud_data"] = c[key]
    result.append(temp)

#pprint(result)
'''
데이터 삽입용


'''
col_result.insert_many(result)


