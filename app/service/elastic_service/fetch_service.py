from typing import List, Dict, Any, Optional, Iterator
from dataclasses import dataclass
import groq
import json
import requests
import time
from threading import Lock
from app.db.elastic.config import settings
from app.db.elastic.models import NewsClassification, NewsCategory, Coordinates
from prometheus_client import Counter, Histogram
import os
from app.service.elastic_service.location_service import get_coordinates
from datetime import datetime, timedelta

# Metrics
news_processed_total = Counter('news_processed_total', 'Total news articles processed')
classification_duration = Histogram('classification_duration_seconds', 'Time spent classifying news')


@dataclass
class ProcessingResult:
    success: bool
    data: Optional[Dict] = None
    error: Optional[str] = None


class RateLimitedGroqClient:
    def __init__(self, api_key: str, tokens_per_minute: int = 4500):
        self.client = groq.Client(api_key=api_key)
        self.tokens_per_minute = tokens_per_minute
        self.lock = Lock()
        self.requests = []
        self.cache_file = "classification_cache.json"
        self.cache = self._load_cache()

    def _load_cache(self) -> Dict:
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            print(f"Error loading classification cache: {e}")
        return {}

    def _save_cache(self):
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, ensure_ascii=False)
        except Exception as e:
            print(f"Error saving classification cache: {e}")

    def _can_make_request(self, tokens_needed: int) -> bool:
        now = time.time()
        # Remove requests older than 1 minute
        self.requests = [t for t in self.requests if now - t < 60]
        return len(self.requests) * 100 + tokens_needed <= self.tokens_per_minute

    def _clean_json_string(self, text: str) -> str:
        """Clean up common JSON string issues."""
        # Remove escaped backslashes before quotes
        text = text.replace('\\"', '"')
        # Remove literal backslashes before underscores
        text = text.replace('\\_', '_')
        # Remove any leading/trailing whitespace
        text = text.strip()
        return text

    def classify(self, title: str, content: str, retries: int = 3) -> Optional[Dict]:
        cache_key = f"{title}:{content}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        with self.lock:
            if not self._can_make_request(2500):
                time.sleep(15)

            for attempt in range(retries):
                try:
                    completion = self.client.chat.completions.create(
                        messages=[
                            {"role": "system",
                             "content": "You are a JSON-only API that must respond with a valid JSON object and nothing else."},
                            {"role": "user", "content": f'''Return ONLY a JSON object:
                            Title: {title}
                            Content: {content}

                            Format: {{"category": "terror_event", "location": "City, Country", "confidence": 0.9}}

                            Rules:
                            1. category must be: "terror_event", "historic_terror", or "general_news"
                            2. location must be: "City, Country" or just "Country"
                            3. confidence must be between 0 and 1'''}
                        ],
                        model="mixtral-8x7b-32768",
                        temperature=0.1,
                        max_tokens=200
                    )

                    response_text = self._clean_json_string(completion.choices[0].message.content)
                    result = json.loads(response_text)

                    if self._validate_classification_result(result):
                        self.cache[cache_key] = result
                        self._save_cache()
                        self.requests.append(time.time())
                        return result
                    else:
                        print(f"Invalid classification result format: {result}")

                except json.JSONDecodeError as e:
                    print(f"JSON parsing error: {str(e)} in response: {response_text}")
                    if attempt == retries - 1:
                        return None

                except Exception as e:
                    if "rate_limit" in str(e).lower():
                        wait_time = 15 * (attempt + 1)
                        print(f"Rate limit hit, waiting {wait_time} seconds...")
                        time.sleep(wait_time)
                    else:
                        print(f"Error during classification: {e}")
                    if attempt == retries - 1:
                        return None
                    continue

            return None


def _validate_classification_result(self, result: Dict) -> bool:
    if not isinstance(result, dict):
        return False

    required_fields = {'category', 'location', 'confidence'}
    if not all(field in result for field in required_fields):
        return False

    if result['category'] not in {'terror_event', 'historic_terror', 'general_news'}:
        return False

    if not isinstance(result['location'], str) or not result['location'].strip():
        return False

    if not isinstance(result['confidence'], (int, float)) or not 0 <= result['confidence'] <= 1:
        return False

    return True


# Global clients
groq_client = RateLimitedGroqClient(settings.GROQ_API_KEY)


def compose_news_request() -> Dict[str, Any]:
    return {
        "action": "getArticles",
        "keyword": "terror attack",
        "articlesPage": 1,
        "articlesCount": settings.MAX_ARTICLES_PER_FETCH,
        "articlesSortBy": "socialScore",
        "articlesSortByAsc": False,
        "dataType": ["news"],
        "apiKey": settings.NEWS_API_KEY
    }


def fetch_articles() -> List[Dict]:
    try:
        response = requests.post(
            "https://eventregistry.org/api/v1/article/getArticles",
            json=compose_news_request()
        )
        response.raise_for_status()
        data = response.json()
        articles = data.get('articles', {}).get('results', [])
        print(f"Fetched {len(articles)} articles")
        return articles
    except Exception as e:
        print(f"Error fetching articles: {e}")
        return []


def safe_process_article(article: Dict) -> ProcessingResult:
    try:
        if not article.get("title") or not article.get("body"):
            return ProcessingResult(False, error="Missing title or body")

        start_time = time.time()
        result = groq_client.classify(
            article.get("title", ""),
            article.get("body", "")
        )
        classification_duration.observe(time.time() - start_time)

        if not result:
            return ProcessingResult(False, error="Classification failed")

        location_parts = result["location"].split(", ")
        city = location_parts[0] if location_parts else None
        country = location_parts[1] if len(location_parts) > 1 else None
        coords = get_coordinates(city, country) if city else None

        article_data = {
            "title": article.get("title"),
            "content": article.get("body"),
            "publication_date": article.get("dateTime"),
            "category": result["category"],
            "location": result["location"],
            "confidence": result["confidence"],
            "source_url": article.get("url")
        }

        if coords:
            article_data["coordinates"] = {
                "lat": coords.lat,
                "lon": coords.lon
            }

        news_processed_total.inc()
        return ProcessingResult(True, data=article_data)
    except Exception as e:
        print(f"Error processing article: {str(e)}")
        return ProcessingResult(False, error=str(e))


def process_news_stream() -> Iterator[ProcessingResult]:
    articles = fetch_articles()
    return filter(lambda x: x.success, map(safe_process_article, articles))


def save_to_elasticsearch(doc: Dict):
    try:
        from app.db.elastic.elastic_connect import elastic_client
        elastic_client.index(index=settings.ES_INDEX_FOR_NEWS, document=doc)
        print(f"Successfully saved article: {doc.get('title')[:50]}...")
    except Exception as e:
        print(f"Error in save_to_elasticsearch: {str(e)}")
        print(f"Document attempted to save: {doc}")


def process_news():
    try:
        results = list(process_news_stream())
        print(f"Processed {len(results)} articles")
        for result in results:
            if result.success and result.data:
                try:
                    save_to_elasticsearch(result.data)
                except Exception as e:
                    print(f"Error saving to elasticsearch: {e}")
    except Exception as e:
        print(f"Error in process_news: {e}")