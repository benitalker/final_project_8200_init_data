from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass(frozen=True)
class SearchParams:
    query: str
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    limit: int = 100
    source: Optional[str] = None