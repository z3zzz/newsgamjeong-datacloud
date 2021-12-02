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
    "자영업": "shop",
    "주식": "stock",
    "친환경": "environment",
    "코로나": "corona",
}



def month_total_keyword_specific():
    result = []
    for keyword in keywords:
        temp = {
            "month": "total",
            "keyword": keyword,
            "wordcloud_data": {}
        }
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


    pprint(result)
'''
테스트용
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

    pprint(result)

'''
테스트용
month_specific_keyword_total()

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

    pprint(result)

'''
테스트용
month_specific_keyword_total()

'''
