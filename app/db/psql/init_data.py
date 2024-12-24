from typing import Dict
import pandas as pd
from sqlalchemy.orm import Session
from app.db.psql.models import (
    AttackType, TargetType, Casualties, Event, Location,
    City, Country, Region, TerroristGroup
)
from app.db.psql.database import session_maker

def standardize_data(df_gtd: pd.DataFrame, df_rand: pd.DataFrame) -> pd.DataFrame:
    df_gtd['source_db'] = 'GTD'
    attack_type_mapping = {
        'Firearms': ('Armed Assault', 2),
        'Explosives/Bombs/Dynamite': ('Bombing/Explosion', 3),
        'Incendiary': ('Facility/Infrastructure Attack', 7),
        'Chemical': ('Facility/Infrastructure Attack', 7),
        'Unknown': ('Unknown', 9),
        'Other': ('Unknown', 9),
        'Vehicle': ('Armed Assault', 2),
        'Sabotage Equipment': ('Facility/Infrastructure Attack', 7),
        'Melee': ('Armed Assault', 2)
    }
    df_gtd['standardized_attack_type'] = df_gtd.get('attacktype1_txt', 'Unknown')
    df_gtd['attack_type_id'] = df_gtd.get('attacktype1', 9)
    df_rand['attack_type_tuple'] = df_rand['weaptype1_txt'].map(
        lambda x: attack_type_mapping.get(x, ('Unknown', 9))
    )
    df_rand['standardized_attack_type'] = df_rand['attack_type_tuple'].apply(lambda x: x[0])
    df_rand['attack_type_id'] = df_rand['attack_type_tuple'].apply(lambda x: x[1])
    df_rand.drop('attack_type_tuple', axis=1, inplace=True)
    df_rand = df_rand.assign(
        success=1,
        suicide=0,
        targtype1=14,
        targtype1_txt='Private Citizens & Property',
        property=0,
        propvalue=None
    )
    common_columns = [
        'iyear', 'imonth', 'iday', 'country_txt', 'city', 'provstate',
        'region_txt', 'latitude', 'longitude', 'nkill', 'nwound',
        'summary', 'gname', 'standardized_attack_type', 'attack_type_id',
        'success', 'suicide', 'targtype1', 'targtype1_txt',
        'property', 'propvalue', 'source_db'
    ]
    for df in [df_gtd, df_rand]:
        for col in common_columns:
            if col not in df.columns:
                df[col] = None
    df_merged = pd.concat([
        df_gtd[common_columns],
        df_rand[common_columns]
    ], ignore_index=True)
    for col in ['country_txt', 'city', 'region_txt', 'gname', 'summary']:
        df_merged[col] = df_merged[col].fillna('Unknown')
    return df_merged

def create_or_get_region(session: Session, region_name: str, lookups: Dict) -> int:
    if region_name not in lookups['regions']:
        region = Region(name=region_name)
        session.add(region)
        session.flush()
        lookups['regions'][region_name] = region.id
    return lookups['regions'][region_name]

def create_or_get_country(session: Session, country_name: str, region_id: int, lookups: Dict) -> int:
    if country_name not in lookups['countries']:
        country = Country(name=country_name, region_id=region_id)
        session.add(country)
        session.flush()
        lookups['countries'][country_name] = country.id
    return lookups['countries'][country_name]

def create_or_get_city(session: Session, city_name: str, country_id: int, province: str, lookups: Dict) -> int:
    city_key = f"{city_name}_{country_id}"
    if city_key not in lookups['cities']:
        city = City(name=city_name, country_id=country_id, province=province)
        session.add(city)
        session.flush()
        lookups['cities'][city_key] = city.id
    return lookups['cities'][city_key]

def create_or_get_terrorist_group(session: Session, group_name: str, lookups: Dict) -> int:
    if group_name not in lookups['terrorist_groups']:
        group = TerroristGroup(group_name=group_name)
        session.add(group)
        session.flush()
        lookups['terrorist_groups'][group_name] = group.id
    return lookups['terrorist_groups'][group_name]

def create_or_get_attack_type(session: Session, name: str, attack_id: int, lookups: Dict) -> int:
    if name not in lookups['attack_types']:
        attack_type = AttackType(id=attack_id, name=name)
        session.add(attack_type)
        session.flush()
        lookups['attack_types'][name] = attack_type.id
    return lookups['attack_types'][name]

def create_or_get_target_type(session: Session, name: str, target_id: int, lookups: Dict) -> int:
    if name not in lookups['target_types']:
        target_type = TargetType(id=target_id, name=name)
        session.add(target_type)
        session.flush()
        lookups['target_types'][name] = target_type.id
    return lookups['target_types'][name]

def seed_database(df: pd.DataFrame):
    with session_maker() as session:
        lookups = {
            'regions': {},
            'countries': {},
            'cities': {},
            'attack_types': {},
            'target_types': {},
            'terrorist_groups': {}
        }
        total_rows = len(df)
        for idx, row in df.iterrows():
            try:
                if idx % 100 == 0:
                    print(f"Processing row {idx}/{total_rows}")
                    session.commit()
                region_id = create_or_get_region(session, row['region_txt'], lookups)
                country_id = create_or_get_country(session, row['country_txt'], region_id, lookups)
                city_id = create_or_get_city(session, row['city'], country_id, row['provstate'], lookups)
                location = Location(
                    latitude=row['latitude'],
                    longitude=row['longitude'],
                    country_id=country_id,
                    city_id=city_id,
                    region_id=region_id
                )
                session.add(location)
                session.flush()
                group_id = create_or_get_terrorist_group(session, row['gname'], lookups)
                attack_type_id = create_or_get_attack_type(
                    session, row['standardized_attack_type'], row['attack_type_id'], lookups
                )
                target_type_id = create_or_get_target_type(
                    session, row['targtype1_txt'], row['targtype1'], lookups
                )
                casualties = Casualties(
                    killed=row['nkill'] or 0,
                    wounded=row['nwound'] or 0,
                    property_damage=row['property'] == 1,
                    property_value=row['propvalue']
                )
                session.add(casualties)
                session.flush()
                event = Event(
                    year=row['iyear'] if row['iyear'] != 2068 else 1968,
                    month=row['imonth'],
                    day=row['iday'],
                    summary=row['summary'],
                    success=row['success'],
                    suicide=row['suicide'],
                    attack_type_id=attack_type_id,
                    target_type_id=target_type_id,
                    casualties_id=casualties.id,
                    location_id=location.id,
                    group_id=group_id
                )
                session.add(event)
                if idx % 100 == 99:
                    session.commit()
            except Exception as e:
                session.rollback()
                print(f"Error processing row {idx}:")
                print(f"Error: {str(e)}")
                print(f"Row: {row}")
                continue
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error in final commit: {str(e)}")
