import datetime
from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote_plus, unquote
import xmltodict
import json
import ElasticsearchService

# Service URL
url = 'http://openapi.data.go.kr/openapi/service/rest/Covid19/getCovid19GenAgeCaseInfJson'
# 인증키
MyApiKey = unquote('jWt83EDDTe%2FlTW%2BAvnwoi1u20sf20LtukAyEU0f95SUVKSGh0N4uCFMSGg3AWbYO2GYVujE5roWbs7ykqe3sOw%3D%3D')

# 구분값
gubunList = ['0-9', '10-19', '20-29', '30-39', '40-49', '50-59', '60-69', '70-79', '80 이상', '남성', '여성']

queryParams = '?' + urlencode({
    quote_plus('ServiceKey'): MyApiKey            # 서비스 키
    , quote_plus('pageNo'): '1'                     # 페이지 번호
    , quote_plus('numOfRows'): '10'                 # 한 페이지 결과 수
    , quote_plus('startCreateDt'): '20201206'       # 데이터 생성일 시작범위
    , quote_plus('endCreateDt'): '20201207'         # 데이터 생성일 종료범위
})

request = Request(url + queryParams)
request.get_method = lambda: 'GET'
response_body = urlopen(request).read()

# XML을 JSON으로 파싱
o = xmltodict.parse(response_body)
json_data = json.loads(json.dumps(o))

# 필요한 json데이터 영역
if int(json_data['response']['body']['totalCount']) > 0:
    data_list = json_data['response']['body']['items']['item']

    # seq 기준 오름차순 정렬
    data_list = sorted(data_list, key=lambda k: int(k.get('seq')), reverse=False)

    # ElasticSearch 호출
    service = ElasticsearchService.ElasticsearchService('age_gender_covid')

    # 인덱스 생성(있는 인덱스는 삭제 후 재생성)
    # service.makeIndex()

    for data in data_list:
        gubun = data['gubun']
        if gubun in gubunList:

            createDt = datetime.datetime.strptime(data['createDt'], '%Y-%m-%d %H:%M:%S.%f') + datetime.timedelta(days=-1)
            covidData = {
                'confCase': int(data['confCase'])               # 확진자
                , 'confCaseRate': float(data['confCaseRate'])   # 확진률
                , 'createDt': createDt.strftime('%Y-%m-%d')     # 확정일
                , 'criticalRate': float(data['criticalRate'])   # 치명률
                , 'death': int(data['death'])                   # 사망자
                , 'deathRate': float(data['deathRate'])         # 사망률
                , 'gubun': data['gubun']                        # 구분
                , 'seq': int(data['seq'])                       # 시퀀스
            }
            # 데이터 삽입
            service.insertCovidData(covidData)
        else:
            print("Error gubun == " + gubun)

    # 인덱스 refresh
    # service.refreshIndex()

    # 데이터 조회
    service.selectCovidData()

else:
    print("조회된 데이터가 없습니다.")

