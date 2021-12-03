import pymongo
import json
from pprint import pprint

## 필수!! db, col 이름 변수 설정
db_from_name = 'final_data'
col_from_name = 'statistics'

db_to_name = "final_data"
col_to_name = 'rankings'

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

def supplement_ranking(news_ranking):
    positive_cnt = len(news_ranking["positive_ranking"])
    negative_cnt = len(news_ranking["negative_ranking"])
    normal_cnt = len(news_ranking["normal_ranking"])
    newses_cnt = len(news_ranking["number_of_newses"])

    if positive_cnt < 5:
        news_ranking["positive_ranking"] += [['',0]] * (5 - positive_cnt)
        news_ranking["negative_ranking"] += [['',0]] * (5 - negative_cnt)
        news_ranking["normal_ranking"] += [['',0]] * (5 - normal_cnt)
        for i in range(5 - newses_cnt):
            news_ranking["number_of_newses"][f'언론사 없음{i+1}'] = 0

    return news_ranking

def convert_count_to_ratio(result2):
    minimum_number, cnt = get_average_number_of_newses(result2)
    result3 = {"positive_ranking": [], "negative_ranking": [], "normal_ranking": [], "number_of_newses": {}, "criteria": minimum_number, "number_of_companies": cnt}

    for company in result2["positive_ranking"].keys():
        positive_count = result2["positive_ranking"][company]
        negative_count = result2["negative_ranking"][company]
        normal_count = result2["normal_ranking"][company]
        total = positive_count + negative_count + normal_count
        if total == 0:
            continue

        positive_ratio = (positive_count / total) * 100
        negative_ratio = (negative_count / total) * 100
        normal_ratio = (normal_count / total) * 100

        if total > minimum_number:
            result3["positive_ranking"].append([company,round(positive_ratio,1)])
            result3["negative_ranking"].append([company,round(negative_ratio,1)])
            result3["normal_ranking"].append([company,round(normal_ratio,1)])
            result3["number_of_newses"][company] = total


    result3["positive_ranking"] = sorted(result3["positive_ranking"], key=lambda x:x[1], reverse=True )[:5]
    result3["negative_ranking"] = sorted(result3["negative_ranking"], key=lambda x:x[1], reverse=True )[:5]
    result3["normal_ranking"] = sorted(result3["normal_ranking"], key=lambda x:x[1], reverse=True )[:5]

    return result3

def get_average_number_of_newses(result2):
    total = 0
    cnt = 0
    for company in result2["positive_ranking"].keys():
        positive_count = result2["positive_ranking"][company]
        negative_count = result2["negative_ranking"][company]
        normal_count = result2["normal_ranking"][company]

        total += positive_count + negative_count + normal_count
        cnt += 1

    print(total,cnt)
    return round(total / cnt), cnt



# 1
def date_total_keyword_specific():
    for keyword in selectors["keywords"]:

        result = {
            "positive_ranking": {},
            "negative_ranking": {},
            "normal_ranking": {},
        }


        for data in col.find({"date": "total", "keyword": keyword, "company":{"$ne":"total"}}):
            company = data["company"]

            if company not in result["positive_ranking"]:
                result["positive_ranking"][company] = data["positive"]
            else:
                result["positive_ranking"][company] += data["positive"]

            if company not in result["negative_ranking"]:
                result["negative_ranking"][company] = data["negative"]
            else:
                result["negative_ranking"][company] += data["negative"]

            if company not in result["normal_ranking"]:
                result["normal_ranking"][company] = data["normal"]
            else:
                result["normal_ranking"][company] += data["normal"]

        result2 = convert_count_to_ratio(result)

        result3 = supplement_ranking(result2)

        result3["date"] = "total"
        result3["keyword"] = keyword

        col_result.insert_one(result3)

        break


    return "done"

'''
업데이트 경우 외 실행 금지
date_total_keyword_specific()
'''

new_dates = ["2021.11.27", "2021.11.28", "2021.11.29", "2021.11.30", "2021.12.01", "2021.12.02"]

# 2
def date_specific_keyword_total():
    for date in new_dates:
    #for date in selectors["dates"]:

        result = {
            "positive_ranking": {},
            "negative_ranking": {},
            "normal_ranking": {},
        }


        for data in col.find({"date": date, "keyword": "total", "company": {"$ne": "total"}}):
            company = data["company"]

            if company not in result["positive_ranking"]:
                result["positive_ranking"][company] = data["positive"]
            else:
                result["positive_ranking"][company] += data["positive"]

            if company not in result["negative_ranking"]:
                result["negative_ranking"][company] = data["negative"]
            else:
                result["negative_ranking"][company] += data["negative"]

            if company not in result["normal_ranking"]:
                result["normal_ranking"][company] = data["normal"]
            else:
                result["normal_ranking"][company] += data["normal"]

        result2 = convert_count_to_ratio(result)

        result3 = supplement_ranking(result2)

        result3["date"] = date
        result3["keyword"] = "total"

        col_result.insert_one(result3)
        print(date)


    return "done"

'''
업데이트 경우 외 실행 금지
date_specific_keyword_total()
'''



# 3
def date_specific_keyword_specific():
    for date in new_dates:
    #for date in selectors["dates"]:
        for keyword in selectors["keywords"]:

            result = {
                "positive_ranking": {},
                "negative_ranking": {},
                "normal_ranking": {},
            }


            for data in col.find({"date": date, "keyword": keyword, "company": {"$ne": "total"}}):
                company = data["company"]

                if company not in result["positive_ranking"]:
                    result["positive_ranking"][company] = data["positive"]
                else:
                    result["positive_ranking"][company] += data["positive"]

                if company not in result["negative_ranking"]:
                    result["negative_ranking"][company] = data["negative"]
                else:
                    result["negative_ranking"][company] += data["negative"]

                if company not in result["normal_ranking"]:
                    result["normal_ranking"][company] = data["normal"]
                else:
                    result["normal_ranking"][company] += data["normal"]

            result2 = convert_count_to_ratio(result)

            result3 = supplement_ranking(result2)

            result3["date"] = date
            result3["keyword"] = keyword

            col_result.insert_one(result3)
            print(date)



    return "done"

'''
업데이트 경우 외 실행 금지
date_specific_keyword_specific()
'''



# 4
def date_total_keyword_total():
    result = {
        "positive_ranking": {},
        "negative_ranking": {},
        "normal_ranking": {},
    }

    for data in col.find({"date": "total", "keyword": "total", "company":{"$ne":"total"}}):
        company = data["company"]

        if company not in result["positive_ranking"]:
            result["positive_ranking"][company] = data["positive"]
        else:
            result["positive_ranking"][company] += data["positive"]

        if company not in result["negative_ranking"]:
            result["negative_ranking"][company] = data["negative"]
        else:
            result["negative_ranking"][company] += data["negative"]

        if company not in result["normal_ranking"]:
            result["normal_ranking"][company] = data["normal"]
        else:
            result["normal_ranking"][company] += data["normal"]

    result2 = convert_count_to_ratio(result)

    result3 = supplement_ranking(result2)

    result3["date"] = "total"
    result3["keyword"] = "total"

    col_result.insert_one(result3)


    return "done"

'''
업데이트 경우 외 실행 금지
date_total_keyword_total()
'''

