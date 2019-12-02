# 시군구별 실시간 평귡어보 조회 오퍼레이션

from datetime import datetime, timedelta
#from urllib.request import urlopen
import xml.etree.ElementTree as ET
import requests
import numpy as np
from elasticsearch import Elasticsearch

TARGET_URL_ENDPOINT="http://openapi.airkorea.or.kr/openapi/services/rest/ArpltnInforInqireSvc/getCtprvnMesureSidoLIst?"
TARGET_URL_SERVICEKEY="ServiceKey="    # 공공데이터포털에서 받아온 API Key
TARGET_URL_NUMOFROWS="&numOfRows="  # 한 페이지 결과 수
TARGET_URL_PAGENO="&pageNo="   # 페이지 번호
TARGET_URL_SIDONAME="&sidoName="    # 시도명
TARGET_URL_SEARCHCONDITION="&searchCondition="    # 데이터 기간

# API Key
SERVICEKEY = "%2FsQMJ9S81k9t5qGUQxLL84cn24R%2Fyl0CcGZfg3%2BJzOlh9PNr0sNlHzV9NjKILYqaC36vg4LW%2BamHp0e49UoKkA%3D%3D"

# 한 페이지 결과 수
# 서울 기준이라 구의 갯수만큼..(그러나 여유롭게)
NUMOFROWS = "100"

# 페이지 번호 (한 페이지에 모두 나오게 설정)
PAGENO = "1"

# 시도명 (서울)
SIDONAME = "서울"

# 데이터 기간 (매 시간 돌리기 위해 시간)
SEARCHCONDITION = "HOUR"

# 현재 날짜 가져오기
NOW_DATETIME = datetime.now().strftime("%Y%m%d")

# 엘라스틱서치에 저장
def es_insert(content):
    print("es_insert")
    try :
        conn = Elasticsearch(hosts="168.1.1.195", port=9200)
        conn.index(index="api_air_ctprvn_real", body=content)
        #conn.index(index="crawling_testtt_words", body=content, id=content["crawling_url"].split("/")[-1].replace(".html", ""))

    except Exception as ex:
        print("엘라스틱 서치 에러 발생", ex)
        return None

# 가져온 데이터 정제하기
def clean_data(note, URL):
    sigungu_cd = {
        "강남구" : "11230",
        "강동구" : "11250",
        "강북구" : "11090",
        "강서구" : "11160",
        "관악구" : "11210",
        "광진구" : "11050",
        "구로구" : "11170",
        "금천구" : "11180",
        "노원구" : "11110",
        "도봉구" : "11100",
        "동대문구" : "11060",
        "동작구" : "11200",
        "마포구" : "11140",
        "서대문구" : "11130",
        "서초구" : "11220",
        "성동구" : "11040",
        "성북구" : "11080",
        "송파구" : "11240",
        "양천구" : "11150",
        "영등포구" : "11190",
        "용산구" : "11030",
        "은평구" : "11120",
        "종로구" : "11010",
        "중구" : "11020",
        "중랑구" : "11070"
    }

    # 각 대기별 오염지수 기준
    '''
    1. 초미세먼지 (PM2.5)
     0 ~ 15 : 좋음
     16 ~ 35 : 보통
     36 ~ 75 : 나쁨
     76 ~ : 매우 나쁨
    
    2. 미세먼저 (PM10)
     0 ~ 30 : 좋음
     31 ~ 80 : 보통
     81 ~ 150 : 나쁨
     151 ~ : 매우나쁨
    
    3. 오존 (O3)
     0 ~ 0.03 : 좋음
     0.031 ~ 0.09 : 보통
     0.091 ~ 0.15 : 나쁨
     0.151 ~ : 매우 나쁨
    
    4. 이산화질소 (NO2)
     0 ~ 0.03 : 좋음
     0.031 ~ 0.06 : 보통
     0.061 ~ 0.2 : 나쁨
     0.201 ~ : 매우 나쁨
    
    5. 일산화탄소 (CO)
     0 ~ 2 : 좋음
     2.01 ~ 9 : 보통
     9.01 ~ 15 : 나쁨
     15.01 ~ : 매우 나쁨
    
    6. 아황산가스 (SO2)
     0 ~ 0.02 : 좋음
     0.021 ~ 0.05 : 보통
     0.051 ~ 0.15 : 나쁨
     0.151 ~ : 매우 나쁨

    '''
    std_pm25 = np.array([16.0, 36.0, 76.0])
    std_pm10 = np.array([31.0, 81.0, 151.0])
    std_o3 = np.array([0.031, 0.091, 0.151])
    std_no2 = np.array([0.031, 0.061, 0.201])
    std_co = np.array([2.01, 9.01, 15.01])
    std_so2 = np.array([0.021, 0.051, 0.151])

    # 기준 설정
    std_arr = [std_so2, std_co, std_o3, std_no2, std_pm10, std_pm25]
    air_arr = ["so2Value", "coValue", "o3Value", "no2Value", "pm10Value", "pm25Value"]
    grade = ["좋음", "보통", "나쁨", "매우 나쁨"]
    grade_cd = [1, 2, 3, 4]

    dummy_date = ""

    for item in note.iter("item"):
        # 상태코드를 담을 배열
        realAirResultCode = []
        # 상태를 담을 배열
        realAirResult = []
        # 평균농도를 담을 배열
        realAir = []
        
        for i in range(6):
            if item.findtext(air_arr[i]) == "-":
                realAir.append(0)
                realAirResult.append("없음")
                realAirResultCode.append(1)
            else:
                realAir.append(float(item.findtext(air_arr[i])))
                d = np.digitize(np.array([float(item.findtext(air_arr[i]))]), std_arr[i])
                realAirResult.append(grade[d[0]])
                realAirResultCode.append(grade_cd[d[0]])

        '''
        content = {
            "apiURL": URL,
            "측정일시": datetime.strptime(item.findtext("dataTime")+":00", "%Y-%m-%d %H:%M:%S"),
            "시군구": item.findtext("cityName"),
            "sigungu_cd" : sigungu_cd[item.findtext("cityName")],
            "아황산가스 평균농도": float(item.findtext("so2Value")),
            "아황산가스 상태" : realAirResult[0],
            "일산화탄소 평균농도": float(item.findtext("coValue")),
            "일산화탄소 상태" : realAirResult[1],
            "오존 평균농도": float(item.findtext("o3Value")),
            "오존 상태" : realAirResult[2],
            "이산화질소 평균농도": float(item.findtext("no2Value")),
            "이산화질소 상태" : realAirResult[3],
            "미세먼지 평균농도": int(item.findtext("pm10Value")),
            "미세먼지 상태" : realAirResult[4],
            "초미세먼지 평균농도": int(item.findtext("pm25Value")),
            "초미세먼지 상태": realAirResult[5]
        }
        '''
        content = {
            "apiURL": URL,
            "측정일시": datetime.strptime(item.findtext("dataTime") + ":00", "%Y-%m-%d %H:%M:%S") + timedelta(hours=-9),
            "시군구": item.findtext("cityName"),
            "sigungu_cd": sigungu_cd[item.findtext("cityName")],
            "아황산가스 평균농도": realAir[0],
            "아황산가스 상태코드": realAirResultCode[0],
            "아황산가스 상태": realAirResult[0],
            "일산화탄소 평균농도": realAir[1],
            "일산화탄소 상태코드": realAirResultCode[1],
            "일산화탄소 상태": realAirResult[1],
            "오존 평균농도": realAir[2],
            "오존 상태코드": realAirResultCode[2],
            "오존 상태": realAirResult[2],
            "이산화질소 평균농도": realAir[3],
            "이산화질소 상태코드": realAirResultCode[3],
            "이산화질소 상태": realAirResult[3],
            "미세먼지 평균농도": realAir[4],
            "미세먼지 상태코드": realAirResultCode[4],
            "미세먼지 상태": realAirResult[4],
            "초미세먼지 평균농도": realAir[5],
            "초미세먼지 상태코드": realAirResultCode[5],
            "초미세먼지 상태": realAirResult[5]
        }

        dummy_date = datetime.strptime(item.findtext("dataTime") + ":00", "%Y-%m-%d %H:%M:%S") + timedelta(hours=-9)
        print(content)
        # 엘라스틱서치에 저장
        es_insert(content)

    # 더미값 생성
    es_insert({"apiURL": URL, "측정일시": dummy_date, "시군구": "dummy_max","sigungu_cd": "dummy_max","아황산가스 평균농도": 0,"아황산가스 상태코드": 5,"아황산가스 상태": "dummy_max","일산화탄소 평균농도": 0,"일산화탄소 상태코드": 5,"일산화탄소 상태": "dummy_max","오존 평균농도": 0,"오존 상태코드": 5,"오존 상태": "dummy_max","이산화질소 평균농도": 0,"이산화질소 상태코드": 5,"이산화질소 상태": "dummy_max","미세먼지 평균농도": 0,"미세먼지 상태코드": 5,"미세먼지 상태": "dummy_max","초미세먼지 평균농도": 0,"초미세먼지 상태코드": 5,"초미세먼지 상태": "dummy_max"})
    es_insert({"apiURL": URL, "측정일시": dummy_date, "시군구": "dummy_min", "sigungu_cd": "dummy_min", "아황산가스 평균농도": 0, "아황산가스 상태코드": 1,"아황산가스 상태": "dummy_min", "일산화탄소 평균농도": 0, "일산화탄소 상태코드": 1, "일산화탄소 상태": "dummy_min", "오존 평균농도": 0, "오존 상태코드": 1,"오존 상태": "dummy_min", "이산화질소 평균농도": 0, "이산화질소 상태코드": 1, "이산화질소 상태": "dummy_min", "미세먼지 평균농도": 0, "미세먼지 상태코드": 1,"미세먼지 상태": "dummy_min", "초미세먼지 평균농도": 0, "초미세먼지 상태코드": 1, "초미세먼지 상태": "dummy_min"})


# API 데이터 가져오기
def get_content_from_url(URL):

    try :
        returnCode = ""
        errMsg = ""

        response = requests.get(URL).text
        print(URL)
        tree = ET.ElementTree(ET.fromstring(response))
        note = tree.getroot()
        resultCode = note.findtext("header/resultCode")

        if resultCode == "00":
            return note
        else:
            returnCode = note.findtext("header/resultCode")
            errMsg = note.findtext("header/resultMsg")
            raise Exception

    except Exception as ex :
        print("API 데이터 가져오기 에러", ex)
        print("리턴코드: ", returnCode, ", 에러메시지: ", errMsg)
        return None

# 메인 함수
def main():
    # API 데이터 가져오기
    #print(NOW_DATETIME)
    TARGET_URL = TARGET_URL_ENDPOINT + TARGET_URL_SERVICEKEY + SERVICEKEY + TARGET_URL_NUMOFROWS + NUMOFROWS + TARGET_URL_PAGENO + PAGENO + TARGET_URL_SIDONAME + SIDONAME + TARGET_URL_SEARCHCONDITION + SEARCHCONDITION
    print(TARGET_URL)

    url_content = get_content_from_url(TARGET_URL)

    # 가져온 데이터 정제하기
    clean_data(url_content, TARGET_URL)

    # 데이터베이스 저장

if __name__ == "__main__":
    main()