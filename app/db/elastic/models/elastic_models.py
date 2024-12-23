from dataclasses import dataclass
from enum import Enum
from typing import Optional

@dataclass(frozen=True)
class Coordinates:
    latitude: float
    longitude: float

class NewsCategory(str, Enum):
    TERROR_EVENT = "terror_event"
    HISTORIC_TERROR = "historic_terror"
    GENERAL_NEWS = "general_news"

@dataclass(frozen=True)
class NewsClassification:
    category: NewsCategory
    location: str
    confidence: float
    coordinates: Optional[Coordinates] = None