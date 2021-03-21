from elasticsearch import Elasticsearch


class ElasticsearchService:
    # 생성자 선언
    def __init__(self, index_name):
        self.index_name = index_name
        self.es = Elasticsearch('http://127.0.0.1:9200', timeout=30, max_retries=10, retry_on_timeout=True)
        self.es.info()

    # 인덱스 생성
    def makeIndex(self):
        # 인덱스가 있는지 체크
        if self.es.indices.exists(index=self.index_name):
            # 인덱스가 있으면 해당 인덱스 삭제
            self.es.indices.delete(index=self.index_name)
        # 인덱스 생성
        print(self.es.indices.create(index=self.index_name))

    # 데이터 저장
    def insertCovidData(self, json_data):

        # 중복 데이터가 있는지 확인 (기준은 구분, 날짜)
        results = self.es.search(index=self.index_name, body={'from': 0, 'size': 1000, "query": {
                                                                                            "bool": {
                                                                                              "must": [
                                                                                                {
                                                                                                  "match": {
                                                                                                    "createDt": json_data['createDt']
                                                                                                  }
                                                                                                },
                                                                                                {
                                                                                                  "match": {
                                                                                                    "gubun": json_data['gubun']
                                                                                                  }
                                                                                                }
                                                                                              ]
                                                                                            }
                                                                                          }})

        if len(results['hits']['hits']) > 0:
            # 중복 데이터 수 만큼 삭제
            for result in results['hits']['hits']:
                print("Delete CreateDt = ", result['_source']['createDt'])
                print("Delete Gubun = ", result['_source']['gubun'])
                # 기존 데이터 삭제
                self.es.delete(index=self.index_name, doc_type='_doc', id=result['_id'])

        # 데이터 저장
        self.es.index(index=self.index_name, doc_type='_doc', body=json_data)
        self.es.indices.refresh(index=self.index_name)

    # 새로고침
    def refreshIndex(self):
        self.es.indices.refresh(index=self.index_name)

    # 데이터 조회
    def selectCovidData(self):
        # results = self.es.search(index=self.index_name, body={'from': 0, 'size': 1000, 'query': {'match_all': {}}})
        results = self.es.search(index=self.index_name, body={'from': 0, 'size': 1000, 'query': {'match': {'createDt': '2020-12-06'}}})
        for result in results['hits']['hits']:
            print(result)
