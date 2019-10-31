# 식중독지수 API 크롤링

from datetime import datetime, timedelta
from urllib.request import urlopen
import requests
import xml.etree.ElementTree as ET
from elasticsearch import Elasticsearch

TARGET_URL_ENDPOINT="http://newsky2.kma.go.kr/iros/RetrieveLifeIndexService3/getFsnLifeList?"
TARGET_URL_SERVICEKEY="ServiceKey="    # 공공데이터포털에서 받아온 API Key
TARGET_URL_AREANO="&areaNo="  # 서울지점
TARGET_URL_TYPE="&_type="   # 응답 값 형식 xml, json
TARGET_URL_TIME="&time="    # 시간값 입력 (YYYYMMDDHH)

# API Key
SERVICEKEY = "%2FsQMJ9S81k9t5qGUQxLL84cn24R%2Fyl0CcGZfg3%2BJzOlh9PNr0sNlHzV9NjKILYqaC36vg4LW%2BamHp0e49UoKkA%3D%3D"

# 지점 가져오기
AREANO = "1100000000"

# 응답 값 형식 가져오기 xml, json
TYPE = "xml"

# 현재 날짜 가져오기
NOW_DATETIME = datetime.now().strftime("%Y%m%d%H")
#NOW_DATETIME = "2017092812"


# 엘라스틱서치에 저장
def es_insert(content):
    print("es_insert")
    try :
        conn = Elasticsearch(hosts="168.1.1.195", port=9200)
        conn.index(index="api_kma_fsn_"+NOW_DATETIME, body=content)
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
def clean_data(content):
    clean_content = content

    # 지수에 값이 없는 경우 0 으로 변경, 있으면 숫자형(int)로 변경
    if content["fsnToday"] == "":
        clean_content["fsnToday"] = 0
    else:
        clean_content["fsnToday"] = int(content["fsnToday"])

    if content["fsnTomorrow"] == "":
        clean_content["fsnTomorrow"] = 0
    else:
        clean_content["fsnTomorrow"] = int(content["fsnTomorrow"])

    if content["fsnTheDayAfterTomorrow"] == "":
        clean_content["fsnTheDayAfterTomorrow"] = 0
    else:
        clean_content["fsnTheDayAfterTomorrow"] = int(content["fsnTheDayAfterTomorrow"])

    # Kibana에서는 날짜 형태가 한국 시간이 아닌 (+9시간) 형태라서 변경해줘야함
    clean_content["fsnDate"] = datetime.strptime(content["fsnDate"] + "0000", "%Y%m%d%H%M%S") + timedelta(hours=-9)

    return clean_content

# API 데이터 가져오기
def get_content_from_url(URL):
    try :
        returnCode = ""
        errMsg = ""

        response = urlopen(URL).read().decode("utf-8")
        print(URL)
        tree = ET.ElementTree(ET.fromstring(response))
        note = tree.getroot()
        successYn = note.findtext("Header/SuccessYN")

        if successYn == "Y":
            print("찾기 성공")
            content = {
                "apiURL"                 : URL,
                "fsnCode"                : note.findtext("Body/IndexModel/code"),
                "fsnAreaNo"              : note.findtext("Body/IndexModel/areaNo"),
                "fsnDate"                : note.findtext("Body/IndexModel/date"),
                "fsnToday"               : note.findtext("Body/IndexModel/today"),
                "fsnTomorrow"            : note.findtext("Body/IndexModel/tomorrow"),
                "fsnTheDayAfterTomorrow" : note.findtext("Body/IndexModel/theDayAfterTomorrow")
            }
            return content

        else:
            returnCode = note.findtext("Header/ReturnCode")
            errMsg = note.findtext("Header/ErrMsg")
            raise Exception
    except Exception as ex:
        print("API 데이터 가져오기 에러", ex)
        print("리턴코드: ", returnCode, ", 에러메시지: ", errMsg)
        return None


# 메인 함수
def main():
    # API 데이터 가져오기
    print(NOW_DATETIME)
    TARGET_URL = TARGET_URL_ENDPOINT + TARGET_URL_SERVICEKEY + SERVICEKEY + TARGET_URL_AREANO + AREANO + TARGET_URL_TYPE + TYPE + TARGET_URL_TIME + NOW_DATETIME
    url_content = get_content_from_url(TARGET_URL)
    #print(url_content)

    # 가져온 데이터 정제하기
    content = clean_data(url_content)
    print(content)

    # 데이터베이스 저장

    # 엘라스틱서치에 저장
    es_insert(content)


if __name__ == "__main__":
    main()