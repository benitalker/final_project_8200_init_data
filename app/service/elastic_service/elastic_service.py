from typing import List

from app.db.elastic.config import Config
from app.db.elastic.elastic_connect import elastic_client
from app.db.elastic.models import TerrorEvent


def save_events_for_news(events: List[TerrorEvent]) -> None:
    for event in events:
        try:
            elastic_client.index(
                index=Config.ES_INDEX_FOR_NEWS,
                document=event.to_elastic_doc()
            )
        except Exception as e:
            print(f"Error saving event: {e}")

def save_events_for_terror(events: List[TerrorEvent]) -> None:
    for event in events:
        try:
            elastic_client.index(
                index=Config.ES_INDEX_FOR_TERROR,
                document=event.to_elastic_doc()
            )
        except Exception as e:
            print(f"Error saving event: {e}")