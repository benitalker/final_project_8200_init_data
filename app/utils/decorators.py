from functools import wraps
import time

def rate_limit(max_per_hour: int):
    def decorator(func):
        calls = []
        @wraps(func)
        def wrapper(*args, **kwargs):
            now = time.time()
            calls[:] = [t for t in calls if now - t < 3600]
            if len(calls) >= max_per_hour:
                wait_time = calls[0] + 3600 - now
                time.sleep(wait_time)
            calls.append(now)
            return func(*args, **kwargs)
        return wrapper
    return decorator