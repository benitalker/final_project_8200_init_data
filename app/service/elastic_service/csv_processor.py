from typing import List, Optional, Iterator
from datetime import datetime
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from toolz import pipe, curry

from app.db.elastic.models import Coordinates, DataSource, TerrorEvent
from app.service.elastic_service.location_service import get_coordinates


def create_terror_event(
        title: str,
        content: str,
        date: datetime,
        location: str,
        coordinates: Optional[Coordinates],
        source: DataSource
) -> TerrorEvent:
    return TerrorEvent(
        title=title,
        content=content,
        publication_date=date,
        category="historic_terror",
        location=location,
        confidence=1.0,
        source_url=source.value,
        coordinates=coordinates
    )


def process_main_csv(filepath: str, limit: int = None, batch_size: int = 1000) -> List[TerrorEvent]:
    print(f"Reading main CSV file: {filepath}")
    df = pd.read_csv(filepath, encoding='iso-8859-1', nrows=limit)
    events = []

    # Process in batches
    for start_idx in range(0, len(df), batch_size):
        batch_df = df.iloc[start_idx:start_idx + batch_size]
        batch_events = process_main_batch(batch_df)
        events.extend(batch_events)
        print(f"Processed {len(events)} events from main CSV")

    return events


def process_secondary_csv(filepath: str, limit: int = None, batch_size: int = 1000) -> List[TerrorEvent]:
    print(f"Reading secondary CSV file: {filepath}")
    df = pd.read_csv(filepath, encoding='iso-8859-1', nrows=limit)
    events = []

    # Process in batches
    for start_idx in range(0, len(df), batch_size):
        batch_df = df.iloc[start_idx:start_idx + batch_size]
        batch_events = process_secondary_batch(batch_df)
        events.extend(batch_events)
        print(f"Processed {len(events)} events from secondary CSV")

    return events


@curry
def process_main_batch(df: pd.DataFrame) -> List[TerrorEvent]:
    events = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for _, row in df.iterrows():
            futures.append(executor.submit(process_main_row, row))

        for future in futures:
            try:
                event = future.result()
                if event:
                    events.append(event)
            except Exception as e:
                print(f"Error processing main CSV row: {e}")
    return events


@curry
def process_secondary_batch(df: pd.DataFrame) -> List[TerrorEvent]:
    events = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for _, row in df.iterrows():
            futures.append(executor.submit(process_secondary_row, row))

        for future in futures:
            try:
                event = future.result()
                if event:
                    events.append(event)
            except Exception as e:
                print(f"Error processing secondary CSV row: {e}")
    return events


def process_main_row(row: pd.Series) -> Optional[TerrorEvent]:
    try:
        date = parse_date(row['iyear'], row['imonth'], row['iday'])
        if not date:
            return None

        coordinates = extract_coordinates(row)
        location = f"{row['city']}, {row['country_txt']}" if pd.notna(row['city']) else row['country_txt']

        return create_terror_event(
            title=f"Terror Attack in {location}",
            content=create_content(row),
            date=date,
            location=location,
            coordinates=coordinates,
            source=DataSource.MAIN_CSV
        )
    except Exception as e:
        print(f"Error processing main row: {e}")
        return None


def process_secondary_row(row: pd.Series) -> Optional[TerrorEvent]:
    try:
        date = datetime.strptime(row['Date'], '%d-%b-%y')
        location = f"{row['City']}, {row['Country']}" if pd.notna(row['City']) else row['Country']
        coordinates = get_coordinates(row['City'], row['Country']) if pd.notna(row['City']) else None

        return create_terror_event(
            title=f"Terror Attack in {location}",
            content=clean_text(row['Description']),
            date=date,
            location=location,
            coordinates=coordinates,
            source=DataSource.SECONDARY_CSV
        )
    except Exception as e:
        print(f"Error processing secondary row: {e}")
        return None


def parse_date(year: int, month: Optional[int], day: Optional[int]) -> Optional[datetime]:
    try:
        return datetime(
            year=int(year),
            month=int(month) if pd.notna(month) else 1,
            day=int(day) if pd.notna(day) else 1
        )
    except ValueError:
        return None


def extract_coordinates(row: pd.Series) -> Optional[Coordinates]:
    if pd.notna(row['latitude']) and pd.notna(row['longitude']):
        return Coordinates(
            lat=float(row['latitude']),
            lon=float(row['longitude'])
        )
    return get_coordinates(row['city'], row['country_txt'])


def clean_text(text: str) -> str:
    if pd.isna(text):
        return ""
    try:
        return str(text).strip()
    except:
        return ""


def create_content(row: pd.Series) -> str:
    if pd.notna(row.get('summary')):
        return clean_text(row['summary'])
    attack_type = clean_text(row.get('attacktype1_txt', ''))
    target_type = clean_text(row.get('targtype1_txt', ''))
    if attack_type and target_type:
        return f"{attack_type} attack targeting {target_type}"
    return "Terror incident"