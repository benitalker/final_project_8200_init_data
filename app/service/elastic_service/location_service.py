from typing import Optional, Dict, Tuple
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
from app.db.elastic.models import Coordinates
import time
from threading import Lock
import os
import json


class RateLimitedGeocoder:
    def __init__(self, user_agent: str, min_delay_seconds: float = 1.0):
        self.geocoder = Nominatim(
            user_agent=user_agent,
            timeout=10
        )
        self.min_delay_seconds = min_delay_seconds
        self.last_request_time = 0
        self.lock = Lock()
        self.cache_file = "geocoding_cache.json"
        self.cache = self._load_cache()

        # Historical country mappings
        self.country_corrections = {
            'USSR': 'Russia',
            'Federal Republic of Germany': 'Germany',
            'Sri Lanka (Ceylon)': 'Sri Lanka',
            'West Germany': 'Germany',
            'East Germany': 'Germany',
            'Yugoslavia': 'Serbia',
        }

    def _load_cache(self) -> Dict[str, Tuple[float, float]]:
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            print(f"Error loading geocoding cache: {e}")
        return {}

    def _save_cache(self):
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, ensure_ascii=False)
        except Exception as e:
            print(f"Error saving geocoding cache: {e}")

    def _correct_country(self, country: str) -> str:
        return self.country_corrections.get(country, country)

    def geocode(self, location_query: str, retries: int = 3) -> Optional[Tuple[float, float]]:
        # Check cache first
        if location_query in self.cache:
            return tuple(self.cache[location_query])

        with self.lock:
            # Enforce rate limiting
            current_time = time.time()
            time_since_last_request = current_time - self.last_request_time
            if time_since_last_request < self.min_delay_seconds:
                time.sleep(self.min_delay_seconds - time_since_last_request)

            for attempt in range(retries):
                try:
                    location = self.geocoder.geocode(location_query)
                    self.last_request_time = time.time()

                    if location:
                        coords = (location.latitude, location.longitude)
                        self.cache[location_query] = coords
                        self._save_cache()
                        return coords

                    # If location not found, try without parenthetical information
                    if '(' in location_query and attempt == 0:
                        cleaned_query = location_query.split('(')[0].strip()
                        return self.geocode(cleaned_query, retries=retries - 1)

                    return None

                except (GeocoderTimedOut, GeocoderUnavailable) as e:
                    if attempt == retries - 1:
                        print(f"Geocoding error for {location_query}: {e}")
                        return None
                    time.sleep((attempt + 1) * 2)  # Exponential backoff
                    continue

                except Exception as e:
                    print(f"Unexpected error geocoding {location_query}: {e}")
                    return None


# Global geocoder instance
geocoder = RateLimitedGeocoder("terror_analysis", min_delay_seconds=2.0)  # Increased delay to be more conservative


def get_coordinates(city: str, country: str = None) -> Optional[Coordinates]:
    try:
        if not city and not country:
            return None

        location_query = city if not country else f"{city}, {country}"

        # Handle historical country names
        if country:
            country = geocoder._correct_country(country)
            location_query = f"{city}, {country}"

        coords = geocoder.geocode(location_query)
        if coords:
            return Coordinates(
                lat=coords[0],
                lon=coords[1]
            )

        # If city+country fails, try just the city
        if country and city:
            coords = geocoder.geocode(city)
            if coords:
                return Coordinates(
                    lat=coords[0],
                    lon=coords[1]
                )

        return None

    except Exception as e:
        print(f"Error getting coordinates for {location_query}: {e}")
        return None