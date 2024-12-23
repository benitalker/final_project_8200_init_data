from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum

@dataclass(frozen=True)
class Coordinates:
    lat: float
    lon: float

    def to_dict(self) -> Dict[str, float]:
        return {
            "lat": self.lat,
            "lon": self.lon
        }

class DataSource(str, Enum):
    MAIN_CSV = "historic_dataset"
    SECONDARY_CSV = "historic_dataset_2"

@dataclass(frozen=True)
class TerrorEvent:
    title: str
    content: str
    publication_date: datetime
    category: str
    location: str
    confidence: float
    source_url: str
    coordinates: Optional[Coordinates]

    def to_elastic_doc(self) -> Dict[str, Any]:
        return {
            "title": self.title,
            "content": self.content,
            "publication_date": self.publication_date.isoformat(),
            "category": self.category,
            "location": self.location,
            "confidence": self.confidence,
            "source_url": self.source_url,
            "coordinates": self.coordinates.to_dict() if self.coordinates else None
        }