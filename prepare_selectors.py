import pymongo
import json
from pprint import pprint

## 필수!! db, col 이름 변수 설정
db_from_name = 'final_data'
col_from_name = 'newses'

db_to_name = 'final_data'
col_to_name = 'selectors'

connection = pymongo.MongoClient("mongodb://localhost:27017/")
db = connection.get_database(db_from_name)
col = db.get_collection(col_from_name)

db_result = connection.get_database(db_to_name)
col_result = db_result.get_collection(col_to_name)

keywords = ["교육", "IT", "대선","백신","부동산","수도권","여행","자영업","주식","친환경","코로나", "정치", "경제", "사회", "생활문화", "세계", "오피니언"]

dates = []
text_companies = []

def get_selectors():
    for i, c in enumerate(col.find()):
        if c["date"] not in dates:
            dates.append(c["date"])
        if c["text_company"] not in text_companies:
            text_companies.append(c["text_company"])
        print(i)

    col_result.insert_one({
        "keywords": keywords,
        "dates": dates,
        "text_companies": text_companies
    })

get_selectors()

'''
메인함수
'''
