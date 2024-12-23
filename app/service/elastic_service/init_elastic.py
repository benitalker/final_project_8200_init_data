from app.db.elastic.config import Config
from app.db.elastic.elastic_connect import elastic_client


def create_index():
    index_body = {
        "mappings": {
            "properties": {
                "title": {"type": "text"},
                "content": {"type": "text"},
                "publication_date": {"type": "date"},
                "category": {"type": "keyword"},
                "location": {"type": "text"},
                "confidence": {"type": "float"},
                "source_url": {"type": "keyword"},
                "coordinates": {
                    "type": "geo_point"
                }
            }
        }
    }
    if not elastic_client.indices.exists(index=Config.ES_INDEX_FOR_NEWS):
        elastic_client.indices.create(index=Config.ES_INDEX_FOR_NEWS, body=index_body)
        print(f"Created index {Config.ES_INDEX_FOR_NEWS}")