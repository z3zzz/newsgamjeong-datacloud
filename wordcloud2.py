import pymongo
import json
from pprint import pprint
import itertools

## 필수!! db, col 이름 변수 설정
db_from_name = 'final_data'
col_from_name = 'wordcloud'

db_to_name = 'final_data'
col_to_name = 'wordcloud'


connection = pymongo.MongoClient("mongodb://localhost:27017/")

db = connection.get_database(db_from_name)
col = db.get_collection(col_from_name)

db_result = connection.get_database(db_to_name)
col_result = db_result.get_collection(col_to_name)

col_selectors = db.get_collection("selectors")
selectors = col_selectors.find_one({})
keywords = selectors["keywords"]

mapping = {
    "IT": "it",
    "교육": "education",
    "대선": "election",
    "백신": "vaccine",
    "부동산": "realEstate",
    "수도권": "capital",
    "여행": "trip",
    "자영업": "store",
    "주식": "stock",
    "친환경": "environment",
    "코로나": "corona",
}



def month_total_keyword_specific():
    result = []
    for keyword in keywords:
        try:
            temp = {
                "month": "total",
                "keyword": mapping[keyword],
                "wordcloud_data": {}
            }
        except:
            continue

        try:
            for c in col.find({"keyword": mapping[keyword]}):
                for word, count in c["wordcloud_data"].items():
                    if word in temp["wordcloud_data"]:
                        temp["wordcloud_data"][word] += count
                    else:
                        temp["wordcloud_data"][word] = count
        except Exception as e:
            print(e)

        temps = sorted(temp["wordcloud_data"].items(), key=lambda x:x[1], reverse=True)
        temp["wordcloud_data"] = dict(itertools.islice(temps,40))
        result.append(temp)


    col_result.insert_many(result)
'''
데이터 삽잉용
month_total_keyword_specific()
'''


def month_specific_keyword_total():
    result = []
    for i in range(1,12):
        month = str(i)
        temp = {
            "month": month,
            "keyword": "total",
            "wordcloud_data": {}
        }
        for c in col.find({"month": month}):
            for word, count in c["wordcloud_data"].items():
                if word in temp["wordcloud_data"]:
                    temp["wordcloud_data"][word] += count
                else:
                    temp["wordcloud_data"][word] = count

        temps = sorted(temp["wordcloud_data"].items(), key=lambda x:x[1], reverse=True)
        temp["wordcloud_data"] = dict(itertools.islice(temps,40))
        result.append(temp)

    col_result.insert_many(result)

'''
데이터 삽입용
month_specific_keyword_total()
'''

def month_total_keyword_total():
    result = {
        "month": "total",
        "keyword": "total",
        "wordcloud_data": {}
    }
    for c in col.find({}):
        try:
            for word, count in c["wordcloud_data"].items():
                if word in result["wordcloud_data"]:
                    result["wordcloud_data"][word] += count
                else:
                    result["wordcloud_data"][word] = count
        except Exception as e:
            print(e)

    temps = sorted(result["wordcloud_data"].items(), key=lambda x:x[1], reverse=True)
    result["wordcloud_data"] = dict(itertools.islice(temps,40))

    col_result.insert_one(result)

'''
데이터 삽입용
month_total_keyword_total()
'''



'''
데이터 삭제삭제용
col_result.delete_many({"month":"total"})
'''
