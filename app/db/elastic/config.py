from pydantic_settings import BaseSettings
from dotenv import load_dotenv
import os

load_dotenv(verbose=True)


class Settings(BaseSettings):
    NEWS_API_KEY: str = os.getenv('NEWS_API_KEY', '')
    GROQ_API_KEY: str = os.getenv('GROQ_API_KEY', '')
    ES_INDEX_FOR_NEWS: str = os.getenv('ES_INDEX_FOR_NEWS', 'news_index')
    ES_INDEX_FOR_TERROR: str = os.getenv('ES_INDEX_FOR_TERROR', 'terror_index')
    FETCH_INTERVAL_MINUTES: int = 2
    MAX_ARTICLES_PER_FETCH: int = 100
    ES_HOST: str = "http://localhost:9200"
    ES_USER: str = "elastic"
    ES_PASSWORD: str = "123456"

    class Config:
        env_file = '.env'
        # Allow extra fields in the settings
        extra = 'ignore'


settings = Settings()


# For backward compatibility with existing code
class Config:
    NEWS_API_KEY = settings.NEWS_API_KEY
    GROQ_API_KEY = settings.GROQ_API_KEY
    ES_INDEX_FOR_NEWS = settings.ES_INDEX_FOR_NEWS
    ES_INDEX_FOR_TERROR = settings.ES_INDEX_FOR_TERROR