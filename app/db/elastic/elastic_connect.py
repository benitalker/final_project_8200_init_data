from elasticsearch import Elasticsearch

elastic_client = Elasticsearch(
    "http://localhost:9200",
    basic_auth=("elastic", "123456"),
    verify_certs=False
)
