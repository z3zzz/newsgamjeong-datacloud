import pymongo
import json
from pprint import pprint

## 필수!! db, col 이름 변수 설정
db_from_name = 'final_data'
col_from_name = 'newses'

db_to_name = "practice"
col_to_name = 'statistics'

connection = pymongo.MongoClient("mongodb://localhost:27017/")
db = connection.get_database(db_from_name)
col = db.get_collection(col_from_name)

col_selectors = db.get_collection("selectors")
selectors = col_selectors.find_one({})

'''
dates, keywords, text_companies
'''

db_result = connection.get_database(db_to_name)
col_result = db_result.get_collection(col_to_name)


def make_statistics():
    :



'''
메인함수
make_news_list()
테스트용
print(getSentiment([10,6,3]))
pprint(getLineGraphData(1))
pprint(prepare_data_1())
'''
