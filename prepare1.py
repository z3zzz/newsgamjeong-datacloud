import pymongo
import json
from pprint import pprint

## 필수!! db, col 이름 변수 설정
db_from_name = 'practice'
col_from_name = 'new4'

db_to_name = "final_data"
col_to_name = 'newses'

connection = pymongo.MongoClient("mongodb://localhost:27017/")
db = connection.get_database(db_from_name)
col = db.get_collection(col_from_name)

db_result = connection.get_database(db_to_name)
col_result = db_result.get_collection(col_to_name)

keywords = ["교육", "IT", "대선","백신","부동산","수도권","여행","자영업","주식","친환경","코로나"]

def getSentiment(sentiment_softmax_list):
    mapping = {0: "중립", 1:"부정", 2:"긍정"}
    local_max = 0
    for index, softmax in enumerate(sentiment_softmax_list):
        if softmax > local_max:
            sentiment = mapping[index]
            local_max = softmax
    return sentiment

def make_news_list():
    unit = 5000
    dates = []
    for i in range(800):
        result = []
        for c in col.find({}).skip(unit * i).limit(unit):
            c["date"] = c["time"][:10]
            if c["date"] not in dates:
                dates.append(c["date"])
            if c["category"] == "생활/문화":
                c["category"] = "생활문화"
            c["category"] = [c["category"]]
            for keyword in keywords:
                if keyword in c["text_headline"]:
                    c["category"].append(keyword)

            c["label"] = getSentiment(c["label"])
            result.append(c)

        col_result.insert_many(result)
        print(i)
        print(dates)

'''
메인함수
make_news_list()
테스트용
print(getSentiment([10,6,3]))
pprint(getLineGraphData(1))
pprint(prepare_data_1())
'''
