# 동네별 실시간 날씨 현황
from urllib.request import urlopen
import xml.etree.ElementTree as ET
import xmltodict
from datetime import datetime, timedelta, date
from elasticsearch import Elasticsearch


# 충청남도 천안시동남구 광덕면
#http://www.kma.go.kr/wid/queryDFSRSS.jsp?zone=4413132000

TARGET_URL_ENDPOINT = "http://www.kma.go.kr/wid/queryDFSRSS.jsp?zone="

SIDO = "4100000000"
GUGUN = "4150000000"
ZONE = "4150034000"
ELK_SIGUNGU = "31210"

# 현재 날짜 가져오기
NOW_DATETIME = datetime.now().strftime("%Y%m%d%H")


# 엘라스틱서치에 저장
def es_insert(content):
    print("es_insert")
    try :
        conn = Elasticsearch(hosts="168.1.1.195", port=9200)
        conn.index(index="rss_gov_weather_real_"+NOW_DATETIME, body=content)
        #conn.index(index="crawling_testtt_words", body=content, id=content["crawling_url"].split("/")[-1].replace(".html", ""))
        '''
        if not conn:
            print("ES연결 완료")
        else :
            print(conn)
        '''
    except Exception as ex:
        print("엘라스틱 서치 에러 발생", ex)
        return None


# 가져온 데이터 정제하기
def clean_data(note, URL):

    dayHour = ""
    pubDate = note.findtext("channel/pubDate")
    searchDate = datetime.strptime( pubDate.split(" ")[0] + " " + pubDate.split(" ")[1] + " " + pubDate.split(" ")[2] + " " + pubDate.split(" ")[4], "%Y년 %m월 %d일 %H:%M")

    for data in note.iter("data"):

        if data.findtext("hour") == "24":
            dayHour = datetime.strptime(datetime.strftime( date.today() + timedelta(days=1), "%Y-%m-%d %H:%M:%S" ), "%Y-%m-%d %H:%M:%S")
        else:
            dayHour = datetime.strptime(datetime.strftime( date.today(), "%Y-%m-%d "+ data.findtext("hour") +":%M:%S" ), "%Y-%m-%d %H:%M:%S" )

        if data.findtext("day") == "0":
            realDateTime = dayHour
        elif data.findtext("day") == "1":
            realDateTime = dayHour + timedelta(days=1)
        else:
            realDateTime = dayHour + timedelta(days=2)

        '''
        return {
            "rssURL" : URL,
            "realDateTime" : realDateTime,
            "realHour" : data.findtext("hour"),
            "realDay" : data.findtext("day"),
            "realTemp" : data.findtext("temp"),
            "realTmx" : data.findtext("tmx"),
            "realTmn": data.findtext("tmn"),
            "realsky": data.findtext("sky"),
            "realpty": data.findtext("pty"),
            "realWfKor": data.findtext("wfKor"),
            "realWfEn": data.findtext("wfEn"),
            "realPop": data.findtext("pop"),
            "realR12": data.findtext("r12"),
            "realS12": data.findtext("s12"),
            "realWs": data.findtext("ws"),
            "realWd": data.findtext("wd"),
            "realWdKor": data.findtext("wdKor"),
            "realWdEn": data.findtext("wdEn"),
            "realReh": data.findtext("reh"),
            "realR06": data.findtext("r06"),
            "realS06": data.findtext("s06")
        }
        '''

        content = {
            "rssURL": URL,
            "시코드" : SIDO,
            "군코드" : GUGUN,
            "구코드" : ZONE,
            "sigungu_cd" : ELK_SIGUNGU,
            "날씨 업데이트 시간" : searchDate + timedelta(hours=-9),
            "날씨 예보 시간": realDateTime + timedelta(hours=-9),
            "시간": data.findtext("hour"),
            "날짜코드": data.findtext("day"),
            "예보 시간온도": float(data.findtext("temp")),
            "최고온도": float(data.findtext("tmx")),
            "최저온도": float(data.findtext("tmn")),
            "하늘상태코드": data.findtext("sky"),
            "강수상태코드": data.findtext("pty"),
            "날씨한국어": data.findtext("wfKor"),
            "날씨영어": data.findtext("wfEn"),
            "강수확률": int(data.findtext("pop")),
            "12시간 예상강수량": float(data.findtext("r12")),
            "12시간 예상적설량": float(data.findtext("s12")),
            "풍속": float(data.findtext("ws")),
            "풍향": data.findtext("wd"),
            "풍향한국어": data.findtext("wdKor"),
            "풍향영어": data.findtext("wdEn"),
            "습도": int(data.findtext("reh")),
            "6시간 예상강수량": float(data.findtext("r06")),
            "6시간 예상적설량": float(data.findtext("s06"))
        }

        es_insert(content)


# RSS 데이터 가져오기
def get_content_from_url(URL):
    try:
        response = urlopen(URL).read().decode("utf-8")
        tree = ET.ElementTree(ET.fromstring(response))
        note = tree.getroot()

        return note

    except Exception as ex:
        print("데이터 가져오기 오류", ex)
        return None


# 메인함수
def main():
    TARGET_URL = TARGET_URL_ENDPOINT + ZONE
    content = get_content_from_url(TARGET_URL)
    clean_data(content, TARGET_URL)

if __name__ == "__main__":
    main()