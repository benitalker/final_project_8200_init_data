import warnings
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, Union, Tuple
from pathlib import Path

def read_csv_data(
        csv_path: Union[str, Path],
        encoding: str = 'iso-8859-1',
        dtype_mapping: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """Read CSV file with error handling"""
    try:
        kwargs = {'encoding': encoding}
        if dtype_mapping:
            kwargs['dtype'] = dtype_mapping
        df = pd.read_csv(csv_path, **kwargs)
        return df.replace({np.nan: None})
    except FileNotFoundError:
        print(f"Error: File not found - {csv_path}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return pd.DataFrame()


def parse_date_safely(date_str: str) -> tuple:
    """Parse date string into components"""
    try:
        for fmt in ['%d-%b-%y', '%d-%b-%Y', '%Y-%m-%d', '%m/%d/%Y']:
            try:
                date_obj = pd.to_datetime(date_str, format=fmt)
                return date_obj.year, date_obj.month, date_obj.day
            except ValueError:
                continue
        date_obj = pd.to_datetime(date_str)
        return date_obj.year, date_obj.month, date_obj.day
    except:
        return None, None, None


def transform_worldwide_terrorism_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform RAND data to match GTD format"""
    # Process dates
    date_components = df['Date'].apply(parse_date_safely)

    # Map regions based on countries
    region_mapping = {
        'Israel': 'Middle East & North Africa',
        'Iraq': 'Middle East & North Africa',
        'Afghanistan': 'South Asia',
        'Pakistan': 'South Asia',
        'India': 'South Asia',
        'United States': 'North America',
        'United Kingdom': 'Western Europe',
        'France': 'Western Europe',
        'Russia': 'Eastern Europe',
        'China': 'East Asia',
        'Japan': 'East Asia',
        'Australia': 'Australasia & Oceania',
        'Egypt': 'Middle East & North Africa',
        'Nigeria': 'Sub-Saharan Africa',
        'South Africa': 'Sub-Saharan Africa',
        'Brazil': 'South America',
        'Mexico': 'Central America & Caribbean'
    }

    # Create transformed DataFrame
    return pd.DataFrame({
        'iyear': date_components.apply(lambda x: x[0]),
        'imonth': date_components.apply(lambda x: x[1]),
        'iday': date_components.apply(lambda x: x[2]),
        'country_txt': df['Country'].str.strip(),
        'city': df['City'].str.strip(),
        'region_txt': df['Country'].str.strip().map(region_mapping).fillna('Unknown'),
        'nkill': pd.to_numeric(df['Fatalities'], errors='coerce').fillna(0),
        'nwound': pd.to_numeric(df['Injuries'], errors='coerce').fillna(0),
        'summary': df['Description'].fillna('No description available'),
        'gname': df['Perpetrator'].fillna('Unknown'),
        'weaptype1_txt': df['Weapon'].fillna('Unknown'),
        'source_db': 'RAND',
        'provstate': None,
        'latitude': None,
        'longitude': None
    })

def read_and_process_files() -> Tuple[pd.DataFrame, pd.DataFrame]:
    base_path = Path(__file__).resolve().parent.parent / "data"
    gtd_data = read_csv_data(base_path / "globalterrorismdb.csv")
    rand_data = read_csv_data(base_path / "RAND_Database_of_Worldwide_Terrorism_Incidents.csv")
    rand_transformed = transform_worldwide_terrorism_data(rand_data)
    return gtd_data, rand_transformed

if __name__ == "__main__":
    warnings.filterwarnings('ignore', category=UserWarning)
    gtd_data, rand_data = read_and_process_files()
    print("GTD records:", len(gtd_data))
    print("RAND records:", len(rand_data))