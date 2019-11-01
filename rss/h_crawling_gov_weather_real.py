from urllib.request import urlopen
import xml.etree.ElementTree as ET


# 충청남도 천안시동남구 광덕면
#http://www.kma.go.kr/wid/queryDFSRSS.jsp?zone=4413132000

TARGET_URL_ENDPOINT = "http://www.kma.go.kr/wid/queryDFSRSS.jsp?zone="

ZONE = "4413132000"

# RSS 데이터 가져오기
def get_content_from_url(URL):
    try:
        response = urlopen(URL).read().decode("utf-8")
        #print(response)
        tree = ET.ElementTree(ET.fromstring(response))
        note = tree.getroot()

        realDate = note.findtext("channel/pubDate")

        print(realDate)

        for data in note.iter("data"):
            print(data.findtext("hour"))
            '''
            for child in data:
                print(child.tag, ":", child.text)
            print("------")
            '''
            if data.findtext("day") == "0":

            content = {
                "rssURL" : URL,
                "realHour" : data.findtext("hour"),
                ""

            }
    except Exception as ex:
        print("데이터 가져오기 오류", ex)
        return None


# 메인함수
def main():
    TARGET_URL = TARGET_URL_ENDPOINT + ZONE
    get_content_from_url(TARGET_URL)

if __name__ == "__main__":
    main()