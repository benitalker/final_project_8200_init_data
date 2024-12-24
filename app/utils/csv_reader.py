import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, Union, Tuple
from pathlib import Path

GTD_DTYPE_MAPPING = {
    'iyear': 'Int64',
    'imonth': 'Int64',
    'iday': 'Int64',
    'country_txt': 'str',
    'region_txt': 'str',
    'city': 'str',
    'latitude': 'float64',
    'longitude': 'float64',
    'location': 'str',
    'summary': 'str',
    'success': 'Int64',
    'suicide': 'Int64',
    'attacktype1_txt': 'str',
    'nkill': 'float64',
    'nkillus': 'float64',
    'nkillter': 'float64',
    'nwound': 'float64',
    'nwoundus': 'float64',
    'nwoundte': 'float64',
    'gname': 'str'
}

def read_csv_data(
        csv_path: Union[str, Path],
        encoding: str = 'iso-8859-1',
        dtype_mapping: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    try:
        kwargs = {
            'encoding': encoding,
            'low_memory': False
        }
        if dtype_mapping:
            kwargs['dtype'] = dtype_mapping
        if dtype_mapping:
            header_df = pd.read_csv(csv_path, nrows=0, encoding=encoding)
            existing_columns = {col: dtype for col, dtype in dtype_mapping.items()
                                if col in header_df.columns}
            kwargs['dtype'] = existing_columns
        df = pd.read_csv(csv_path, **kwargs)
        return df.replace({np.nan: None})
    except FileNotFoundError:
        print(f"Error: File not found - {csv_path}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return pd.DataFrame()

def parse_date_safely(date_str: str) -> tuple:
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
    try:
        date_components = df['Date'].apply(parse_date_safely)
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
        transformed_df = pd.DataFrame({
            'iyear': date_components.apply(lambda x: x[0]).astype('Int64'),
            'imonth': date_components.apply(lambda x: x[1]).astype('Int64'),
            'iday': date_components.apply(lambda x: x[2]).astype('Int64'),
            'country_txt': df['Country'].str.strip(),
            'city': df['City'].str.strip(),
            'region_txt': df['Country'].str.strip().map(region_mapping).fillna('Unknown'),
            'nkill': pd.to_numeric(df['Fatalities'], errors='coerce').fillna(0).astype('float64'),
            'nwound': pd.to_numeric(df['Injuries'], errors='coerce').fillna(0).astype('float64'),
            'summary': df['Description'].fillna('No description available'),
            'gname': df['Perpetrator'].fillna('Unknown'),
            'weaptype1_txt': df['Weapon'].fillna('Unknown'),
            'source_db': 'RAND',
            'provstate': pd.Series([None] * len(df), dtype='object'),
            'latitude': pd.Series([None] * len(df), dtype='float64'),
            'longitude': pd.Series([None] * len(df), dtype='float64')
        })
        return transformed_df
    except Exception as e:
        print(f"Error transforming RAND data: {e}")
        return pd.DataFrame()

def read_and_process_files() -> Tuple[pd.DataFrame, pd.DataFrame]:
    try:
        base_path = Path(__file__).resolve().parent.parent / "data"
        print("Reading GTD data...")
        gtd_data = read_csv_data(
            base_path / "globalterrorismdb.csv",
            dtype_mapping=GTD_DTYPE_MAPPING
        )
        print(f"Read {len(gtd_data)} GTD records")
        print("Reading RAND data...")
        rand_data = read_csv_data(
            base_path / "RAND_Database_of_Worldwide_Terrorism_Incidents.csv"
        )
        print(f"Read {len(rand_data)} RAND records")
        print("Transforming RAND data...")
        rand_transformed = transform_worldwide_terrorism_data(rand_data)
        print("Data processing complete")
        return gtd_data, rand_transformed
    except Exception as e:
        print(f"Error in read_and_process_files: {e}")
        return pd.DataFrame(), pd.DataFrame()