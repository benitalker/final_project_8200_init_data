from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from datetime import datetime, timedelta
from app.service.elastic_service.fetch_service import process_news
from app.db.elastic.config import settings
from threading import Lock


class NewsScheduler:
    def __init__(self):
        self._lock = Lock()
        self._is_processing = False

    def _process_news_safely(self):
        with self._lock:
            if self._is_processing:
                print("News processing already in progress, skipping...")
                return

            try:
                self._is_processing = True
                process_news()
            finally:
                self._is_processing = False


def setup_scheduler():
    news_scheduler = NewsScheduler()

    # Configure thread pool
    executors = {
        'default': ThreadPoolExecutor(10)
    }

    # Job defaults
    job_defaults = {
        'coalesce': True,
        'max_instances': 1,
        'misfire_grace_time': 30
    }

    # Create and configure scheduler
    scheduler = BackgroundScheduler(
        executors=executors,
        job_defaults=job_defaults,
        timezone='UTC'
    )

    # Add the news processing job
    scheduler.add_job(
        news_scheduler._process_news_safely,
        'interval',
        minutes=settings.FETCH_INTERVAL_MINUTES,
        next_run_time=datetime.now() + timedelta(seconds=5),
        id='process_news',
        replace_existing=True
    )

    scheduler.start()
    print("Scheduler started successfully")
    return scheduler