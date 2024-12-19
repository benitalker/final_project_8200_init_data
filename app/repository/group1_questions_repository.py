from datetime import datetime
import pandas as pd
from sqlalchemy import func, case, desc
from app.db.database import session_maker
from app.db.models import AttackType, Casualties, Event, Region, Location, TerroristGroup, TargetType

def deadliest_attacks_repo(top_n):
    with session_maker() as session:
        query = session.query(
            AttackType.name.label("attack_type"),
            func.sum(
                case(
                    (Casualties.killed.isnot(None), Casualties.killed * 2),
                    else_=0
                ) +
                case(
                    (Casualties.wounded.isnot(None), Casualties.wounded),
                    else_=0
                )
            ).label("casualty_score")
        ).join(
            Event, Event.attack_type_id == AttackType.id
        ).join(
            Casualties, Event.casualties_id == Casualties.id
        ).group_by(
            AttackType.name
        ).order_by(
            desc("casualty_score")
        )
        if top_n:
            query = query.limit(top_n)
        return query.all()

def casualties_by_region_repo(top_n):
    with session_maker() as session:
        query = session.query(
            Region.name.label("region"),
            func.count(Event.id).label("event_count"),
            func.sum(
                case(
                    (Casualties.killed.isnot(None), Casualties.killed * 2),
                    else_=0
                ) +
                case(
                    (Casualties.wounded.isnot(None), Casualties.wounded),
                    else_=0
                )
            ).label("casualty_score"),
            func.avg(Location.latitude).label("lat"),
            func.avg(Location.longitude).label("lon")
        ).join(
            Location, Location.region_id == Region.id
        ).join(
            Event, Event.location_id == Location.id
        ).join(
            Casualties, Event.casualties_id == Casualties.id
        ).group_by(
            Region.name
        ).having(
            func.count(Event.id) > 0
        )
        if top_n:
            query = query.order_by(desc("casualty_score")).limit(top_n)
        return query.all()

def top_casualty_groups_repo():
    with session_maker() as session:
        return session.query(
            TerroristGroup.group_name,
            func.sum(Casualties.killed * 2 + Casualties.wounded).label("total_casualties"),
            func.min(Event.year).label("start_year"),
            func.max(Event.year).label("end_year"),
            func.count(Event.id).label("num_attacks")
        ).join(Event, Event.group_id == TerroristGroup.id
               ).join(Casualties, Event.casualties_id == Casualties.id
                      ).group_by(TerroristGroup.group_name
                                 ).order_by(desc("total_casualties")
                                            ).limit(5).all()

def attack_target_correlation_repo():
    with session_maker() as session:
        return session.query(
            AttackType.name,
            TargetType.name,
            func.count(Event.id).label("event_count")
        ).join(Event, Event.attack_type_id == AttackType.id
               ).join(TargetType, Event.target_type_id == TargetType.id
                      ).group_by(AttackType.name, TargetType.name).all()

def attack_trends_repo(year):
    with session_maker() as session:
        annual_trends = session.query(
            Event.year.label('year'),
            func.count(Event.id).label('attack_count')
        ).filter(Event.year.isnot(None)
                 ).group_by(Event.year).order_by(Event.year).all()
        monthly_trends = session.query(
            Event.month.label('month'),
            func.count(Event.id).label('attack_count')
        ).filter(
            Event.year == year,
            Event.month.isnot(None)
        ).group_by(Event.month).order_by(Event.month).all()
        return annual_trends, monthly_trends

def attack_change_by_region_repo():
    with session_maker() as session:
        attacks_by_region_year = session.query(
            Region.name.label('region'),
            Event.year.label('year'),
            func.count(Event.id).label('attack_count')
        ).join(Location, Location.region_id == Region.id
               ).join(Event, Event.location_id == Location.id
                      ).filter(Event.year.isnot(None)
                      ).group_by('region', Event.year).subquery()

        region_changes = session.query(
            attacks_by_region_year.c.region,
            attacks_by_region_year.c.year.label('current_year'),
            attacks_by_region_year.c.attack_count.label('current_attacks'),
            func.lag(attacks_by_region_year.c.attack_count).over(
                partition_by=[attacks_by_region_year.c.region],
                order_by=attacks_by_region_year.c.year
            ).label('previous_attacks'),
            func.lag(attacks_by_region_year.c.year).over(
                partition_by=[attacks_by_region_year.c.region],
                order_by=attacks_by_region_year.c.year
            ).label('previous_year')
        ).order_by(attacks_by_region_year.c.region, attacks_by_region_year.c.year)
        df = pd.read_sql(region_changes.statement, session.bind)
        return df

def terror_heatmap_repo(time_period,region_filter):
    with session_maker() as session:
        query = session.query(
            Location.latitude,
            Location.longitude,
            Event.year,
            Event.month,
            Region.name.label('region')
        ).join(Event, Event.location_id == Location.id
               ).join(Region, Region.id == Location.region_id
                      ).filter(
            Location.latitude.isnot(None),
            Location.longitude.isnot(None)
        )
        current_year = datetime.now().year
        if time_period == 'month':
            current_month = datetime.now().month
            query = query.filter(
                Event.year == current_year,
                Event.month == current_month
            )
        elif time_period == '3_years':
            query = query.filter(Event.year >= current_year - 3)
        elif time_period == '5_years':
            query = query.filter(Event.year >= current_year - 5)
        if region_filter:
            query = query.filter(Region.name == region_filter)
        return query.all(), current_year

def active_groups_heatmap_repo(region_filter):
    with session_maker() as session:
        if region_filter:
            query = session.query(
                TerroristGroup.group_name,
                func.count(Event.id).label('attack_count'),
                func.avg(Location.latitude).label('avg_lat'),
                func.avg(Location.longitude).label('avg_lon')
            ).join(
                Event, Event.group_id == TerroristGroup.id
            ).join(
                Location, Location.id == Event.location_id
            ).join(
                Region, Region.id == Location.region_id
            ).filter(
                Region.name == region_filter
            ).group_by(
                TerroristGroup.group_name
            ).order_by(
                desc('attack_count')
            ).limit(5)
        else:
            rank_subquery = session.query(
                TerroristGroup.group_name,
                Region.name.label('region_name'),
                func.count(Event.id).label('attack_count'),
                func.avg(Location.latitude).label('avg_lat'),
                func.avg(Location.longitude).label('avg_lon'),
                func.row_number().over(
                    partition_by=Region.name,
                    order_by=desc(func.count(Event.id))
                ).label('rank')
            ).join(
                Event, Event.group_id == TerroristGroup.id
            ).join(
                Location, Location.id == Event.location_id
            ).join(
                Region, Region.id == Location.region_id
            ).group_by(
                TerroristGroup.group_name,
                Region.name
            ).subquery()
            query = session.query(
                rank_subquery.c.group_name,
                rank_subquery.c.region_name,
                rank_subquery.c.attack_count,
                rank_subquery.c.avg_lat,
                rank_subquery.c.avg_lon
            ).filter(
                rank_subquery.c.rank <= 5
            ).order_by(
                rank_subquery.c.region_name,
                rank_subquery.c.attack_count.desc()
            )
        return query.all()

def perpetrators_casualties_correlation_repo():
    with session_maker() as session:
        return session.query(
            Event.id,
            func.count(TerroristGroup.id).label('perpetrator_count'),
            func.sum(
                case(
                    (Casualties.killed.isnot(None), Casualties.killed * 2),
                    else_=0
                ) +
                case(
                    (Casualties.wounded.isnot(None), Casualties.wounded),
                    else_=0
                )
            ).label('total_casualties')
        ).join(TerroristGroup, Event.group_id == TerroristGroup.id
               ).join(Casualties, Event.casualties_id == Casualties.id
                      ).group_by(Event.id).all()

def events_casualties_correlation_repo(region_name):
    with session_maker() as session:
        query = session.query(
            Region.name.label('region'),
            func.count(Event.id).label('event_count'),
            func.sum(
                case(
                    (Casualties.killed.isnot(None), Casualties.killed * 2),
                    else_=0
                ) +
                case(
                    (Casualties.wounded.isnot(None), Casualties.wounded),
                    else_=0
                )
            ).label("total_casualties")
        ).join(Location, Location.region_id == Region.id
               ).join(Event, Event.location_id == Location.id
                      ).join(Casualties, Event.casualties_id == Casualties.id
                             )
        if region_name:
            query = query.filter(Region.name == region_name)
        return query.group_by(Region.name).all()
