text = ['우리여행 같이가요', '우리는 여행을 좋아한다', '우리는 우주여행 가고 싶다', '여행 가자']

for t in text:
    print(t)
    text2 = t.replace('여행', ' 여행 ')
    text3 = text2.replace('  ', ' ')
    print(text3)
