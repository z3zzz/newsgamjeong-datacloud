import pymongo
import json
from pprint import pprint

## 필수!! db, col 이름 변수 설정
db_from_name = 'final_data'
col_from_name = 'newses'

db_to_name = "final_data"
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

sentiment_mapping = {'중립':'normal', '긍정':'positive', '부정':'negative'}

i = 0

# 1
def date_total_keyword_specific_company_specific():
    for keyword in selectors["keywords"]:
        result = []
        for company in selectors["text_companies"]:
            temp = {
                "date": "total",
                "keyword": keyword,
                "company": company,
                "total": 0,
                "positive": 0,
                "negative": 0,
                "normal": 0
            }

            for data in col.find({"category":keyword,"text_company":company}):
                temp["total"] += 1
                temp[sentiment_mapping[data['label']]] += 1

            result.append(temp)

        col_result.insert_many(result)
        print(keyword)

    return "done"

'''
업데이트 경우 외 실행 금지
pprint(date_total_keyword_specific_company_specific())
'''

# 2
def date_specific_keyword_total_company_specific():
    for date in selectors["dates"]:
        result = []
        for company in selectors["text_companies"]:
            temp = {
                "date": date,
                "keyword": "total",
                "company": company,
                "total": 0,
                "positive": 0,
                "negative": 0,
                "normal": 0
            }

            for data in col.find({"date": date, "text_company":company}):
                temp["total"] += 1
                temp[sentiment_mapping[data['label']]] += 1

            result.append(temp)

        col_result.insert_many(result)
        print(date, company)

    return "done"

'''
업데이트 경우 외 실행 금지
date_specific_keyword_total_company_specific()
'''


# 3
def date_specific_keyword_specific_company_total():
    for date in selectors["dates"]:
        result = []
        for keyword in selectors["keywords"]:
            temp = {
                "date": date,
                "keyword": keyword,
                "company": "total",
                "total": 0,
                "positive": 0,
                "negative": 0,
                "normal": 0
            }

            for data in col.find({"date": date, "category":keyword}):
                temp["total"] += 1
                temp[sentiment_mapping[data['label']]] += 1

            result.append(temp)

        col_result.insert_many(result)
        print(date)

    return "done"

'''
업데이트 경우 외 실행 금지
date_specific_keyword_specific_company_total()
'''

# 4
def date_specific_keyword_total_company_total():
    for date in selectors["dates"]:
        result = []
        temp = {
            "date": date,
            "keyword": "total",
            "company": "total",
            "total": 0,
            "positive": 0,
            "negative": 0,
            "normal": 0
        }

        for data in col.find({"date": date}):
            temp["total"] += 1
            temp[sentiment_mapping[data['label']]] += 1

        result.append(temp)
        col_result.insert_many(result)
        print(date)

    return "done"

'''
업데이트 경우 외 실행 금지
date_specific_keyword_total_company_total()
'''

# 5
def date_total_keyword_specific_company_total():
    for keyword in selectors["keywords"]:
        result = []
        temp = {
            "date": "total",
            "keyword": keyword,
            "company": "total",
            "total": 0,
            "positive": 0,
            "negative": 0,
            "normal": 0
        }

        for data in col.find({"category": keyword}):
            temp["total"] += 1
            temp[sentiment_mapping[data['label']]] += 1

        result.append(temp)
        col_result.insert_many(result)
        print(keyword)

    return "done"

'''
업데이트 경우 외 실행 금지
date_total_keyword_specific_company_total()
'''

# 6
def date_total_keyword_total_company_specific():
    for company in selectors["text_companies"]:
        result = []
        temp = {
            "date": "total",
            "keyword": "total",
            "company": company,
            "total": 0,
            "positive": 0,
            "negative": 0,
            "normal": 0
        }

        for data in col.find({"text_company": company}):
            temp["total"] += 1
            temp[sentiment_mapping[data['label']]] += 1

        result.append(temp)
        col_result.insert_many(result)
        print(company)

    return "done"

'''
업데이트 경우 외 실행 금지
date_total_keyword_total_company_specific()
'''

# 7
def date_specific_keyword_specific_company_specific():
    for date in selectors["dates"]:
        for keyword in selectors["keywords"]:
            result = []
            for company in selectors["text_companies"]:
                temp = {
                    "date": date,
                    "keyword": keyword,
                    "company": company,
                    "total": 0,
                    "positive": 0,
                    "negative": 0,
                    "normal": 0
                }

                for data in col.find({"date": date, "category":keyword,"text_company":company}):
                    temp["total"] += 1
                    temp[sentiment_mapping[data['label']]] += 1

                result.append(temp)

            col_result.insert_many(result)
            print(date,keyword)

    return "done"

'''
업데이트 경우 외 실행 금지
date_specific_keyword_specific_company_specific()
'''

# 8
def date_total_keyword_total_company_total():
    result = []
    temp = {
        "date": "total",
        "keyword": "total",
        "company": "total",
        "total": 0,
        "positive": 0,
        "negative": 0,
        "normal": 0
    }

    for data in col.find({}):
        temp["total"] += 1
        temp[sentiment_mapping[data['label']]] += 1

    result.append(temp)
    col_result.insert_many(result)

    return "done"

'''
업데이트 경우 외 실행 금지
date_total_keyword_total_company_total()
'''
