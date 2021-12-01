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
def date_specific_keyword_total_company_specific(date, company):
    result = {
        "total": 0,
        "positive": 0,
        "negative": 0,
        "normal": 0,
        "infections": 0
    }
    data = col.find_one({"날짜":f'{date}.',"키워드":"전체","언론사":company})

    result["total"] += data["전체"]
    result["positive"] += data["긍정"]
    result["negative"] += data["부정"]
    result["normal"] += data["노말"]

    return result

'''
코드 테스트용
pprint(date_specific_keyword_total_company_specific("2021.08.01", "연합뉴스"))
'''

# 3
def date_specific_keyword_specific_company_total(date, keyword):
    result1 = {
        "total": 0,
        "positive": 0,
        "negative": 0,
        "normal": 0,
        "infections": 0,
    }
    result2 = {
        "positive_ranking": {},
        "negative_ranking": {},
        "normal_ranking": {},
    }

    datas = col.find({"날짜":f'{date}.',"키워드":keyword})

    for data in datas:
        news_company = data["언론사"]
        if news_company == "전체":
            continue
        result1["total"] += data["전체"]
        result1["positive"] += data["긍정"]
        result1["negative"] += data["부정"]
        result1["normal"] += data["노말"]

        if news_company not in result2["positive_ranking"]:
            result2["positive_ranking"][news_company] = data["긍정"]
        else:
            result2["positive_ranking"][news_company] += data["긍정"]

        if news_company not in result2["negative_ranking"]:
            result2["negative_ranking"][news_company] = data["부정"]
        else:
            result2["negative_ranking"][news_company] += data["부정"]

        if news_company not in result2["normal_ranking"]:
            result2["normal_ranking"][news_company] = data["노말"]
        else:
            result2["normal_ranking"][news_company] += data["노말"]

    result3 = convert_count_to_ratio(result2)

    result = {
        "statistics": result1,
        "news_ranking": result3,
    }

    result["news_ranking"] = supplement_ranking(result["news_ranking"])

    return result

'''
코드 테스트용
pprint(date_specific_keyword_specific_company_total("2021.01.09", "자영업"))
'''

# 4
def date_specific_keyword_total_company_total(date):
    result1 = {
        "total": 0,
        "positive": 0,
        "negative": 0,
        "normal": 0,
        "infections": 0
    }
    result2 = {
        "positive_ranking": {},
        "negative_ranking": {},
        "normal_ranking": {}
    }

    datas = col.find({"날짜":f'{date}.',"키워드":"전체"})

    for data in datas:
        news_company = data["언론사"]
        if news_company == "전체":
            continue

        result1["total"] += data["전체"]
        result1["positive"] += data["긍정"]
        result1["negative"] += data["부정"]
        result1["normal"] += data["노말"]

        if news_company not in result2["positive_ranking"]:
            result2["positive_ranking"][news_company] = data["긍정"]
        else:
            result2["positive_ranking"][news_company] += data["긍정"]

        if news_company not in result2["negative_ranking"]:
            result2["negative_ranking"][news_company] = data["부정"]
        else:
            result2["negative_ranking"][news_company] += data["부정"]

        if news_company not in result2["normal_ranking"]:
            result2["normal_ranking"][news_company] = data["노말"]
        else:
            result2["normal_ranking"][news_company] += data["노말"]

    result3 = convert_count_to_ratio(result2)

    result = {
        "statistics": result1,
        "news_ranking": result3
    }

    result["news_ranking"] = supplement_ranking(result["news_ranking"])

    return result

'''
코드 테스트용
pprint(date_specific_keyword_total_company_total("2021.01.02"))
'''

# 5
def date_total_keyword_specific_company_total(keyword):
    result1 = {
        "total": 0,
        "positive": 0,
        "negative": 0,
        "normal": 0,
        "infections": 0
    }
    result2 = {
        "positive_ranking": {},
        "negative_ranking": {},
        "normal_ranking": {}
    }

    datas = col.find({"키워드":keyword, "날짜":"전체"})

    for data in datas:
        news_company = data["언론사"]
        if news_company == "전체":
            continue

        result1["total"] += data["전체"]
        result1["positive"] += data["긍정"]
        result1["negative"] += data["부정"]
        result1["normal"] += data["노말"]

        if news_company not in result2["positive_ranking"]:
            result2["positive_ranking"][news_company] = data["긍정"]
        else:
            result2["positive_ranking"][news_company] += data["긍정"]

        if news_company not in result2["negative_ranking"]:
            result2["negative_ranking"][news_company] = data["부정"]
        else:
            result2["negative_ranking"][news_company] += data["부정"]

        if news_company not in result2["normal_ranking"]:
            result2["normal_ranking"][news_company] = data["노말"]
        else:
            result2["normal_ranking"][news_company] += data["노말"]

    result3 = convert_count_to_ratio(result2)

    result = {
        "statistics": result1,
        "news_ranking": result3
    }

    result["news_ranking"] = supplement_ranking(result["news_ranking"])

    return result

'''
코드 테스트용
pprint(date_total_keyword_specific_company_total("코로나"))
'''

# 6
def date_total_keyword_total_company_specific(company):
    result = {
        "total": 0,
        "positive": 0,
        "negative": 0,
        "normal": 0,
        "infections": 0
    }

    data = col.find_one({"날짜":"전체","키워드":"전체","언론사":company})

    result["total"] += data["전체"]
    result["positive"] += data["긍정"]
    result["negative"] += data["부정"]
    result["normal"] += data["노말"]

    return result

'''
코드 테스트용
pprint(date_total_keyword_total_company_specific("조선일보"))
'''

# 7
def date_specific_keyword_specific_company_specific(date, keyword, company):
    result = {
        "total": 0,
        "positive": 0,
        "negative": 0,
        "normal": 0,
        "infections": 0
    }

    data = col.find_one({"날짜":f'{date}.',"키워드": keyword,"언론사":company})

    result["total"] += data["전체"]
    result["positive"] += data["긍정"]
    result["negative"] += data["부정"]
    result["normal"] += data["노말"]

    return result

'''
코드 테스트용
pprint(date_specific_keyword_specific_company_specific("2021.08.11","부동산","조선일보"))
'''

# 8
def date_total_keyword_total_company_total():
    result1 = {
        "total": 0,
        "positive": 0,
        "negative": 0,
        "normal": 0,
        "infections": 0
    }
    result2 = {
        "positive_ranking": {},
        "negative_ranking": {},
        "normal_ranking": {}
    }


    datas = col.find({"키워드":"전체"})

    for data in datas:
        news_company = data["언론사"]
        if news_company == "전체" or data["날짜"] == "전체":
            continue

        result1["total"] += data["전체"]
        result1["positive"] += data["긍정"]
        result1["negative"] += data["부정"]
        result1["normal"] += data["노말"]

        if news_company not in result2["positive_ranking"]:
            result2["positive_ranking"][news_company] = data["긍정"]
        else:
            result2["positive_ranking"][news_company] += data["긍정"]

        if news_company not in result2["negative_ranking"]:
            result2["negative_ranking"][news_company] = data["부정"]
        else:
            result2["negative_ranking"][news_company] += data["부정"]

        if news_company not in result2["normal_ranking"]:
            result2["normal_ranking"][news_company] = data["노말"]
        else:
            result2["normal_ranking"][news_company] += data["노말"]

    result3 = convert_count_to_ratio(result2)

    result = {
        "statistics": result1,
        "news_ranking": result3
    }

    return result




'''
메인함수
make_news_list()
테스트용
print(getSentiment([10,6,3]))
pprint(getLineGraphData(1))
pprint(prepare_data_1())
'''
