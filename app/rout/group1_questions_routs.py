from flask import Blueprint, Response, request
from app.repository.group1_questions_repository import *
from app.service.group1_questions_service import *

stats_blueprint = Blueprint('stats', __name__)

#1
@stats_blueprint.route('/deadliest_attacks')
def deadliest_attacks():
    top_n = request.args.get('top_n', type=int, default=5)
    results = deadliest_attacks_repo(top_n)
    buf = deadliest_attacks_service(results)
    return Response(buf.getvalue(), mimetype='image/png')

#2
@stats_blueprint.route('/casualties_by_region')
def casualties_by_region():
    top_n = request.args.get('top_n', type=int)
    results = casualties_by_region_repo(top_n)
    buf = casualties_by_region_service(results)
    return Response(buf.getvalue(), mimetype='text/html')

#3
@stats_blueprint.route('/top_casualty_groups')
def top_casualty_groups():
    results = top_casualty_groups_repo()
    buf = top_casualty_groups_service(results)
    return Response(buf.getvalue(), mimetype='image/png')

#4
@stats_blueprint.route('/attack_target_correlation')
def attack_target_correlation():
    results = attack_target_correlation_repo()
    buf = attack_target_correlation_service(results)
    return Response(buf.getvalue(), mimetype='image/png')

#5
@stats_blueprint.route('/attack_trends')
def attack_trends():
    year = request.args.get('year', type=int, default=datetime.now().year)
    annual_trends, monthly_trends = attack_trends_repo(year)
    buf = attack_trends_service(annual_trends, monthly_trends,year)
    return Response(buf.getvalue(), mimetype='image/png')

#6
@stats_blueprint.route('/attack_change_by_region')
def attack_change_by_region():
    top_n = request.args.get('top_n', type=int, default=5)
    df = attack_change_by_region_repo()
    buf = attack_change_by_region_service(df, top_n)
    return Response(buf.getvalue(), mimetype='image/png')

#7
@stats_blueprint.route('/terror_heatmap')
def terror_heatmap():
    time_period = request.args.get('period', default='year', type=str)
    region_filter = request.args.get('region', type=str)
    locations, current_year = terror_heatmap_repo(time_period,region_filter)
    buf = terror_heatmap_service(locations, current_year, time_period, region_filter)
    return Response(buf.getvalue(), mimetype='text/html')

#8
@stats_blueprint.route('/active_groups_heatmap')
def active_groups_heatmap():
    region_filter = request.args.get('region', type=str)
    results = active_groups_heatmap_repo(region_filter)
    buf = active_groups_heatmap_service(results,region_filter)
    return Response(buf.getvalue(), mimetype='text/html')

#9
@stats_blueprint.route('/perpetrators_casualties_correlation')
def perpetrators_casualties_correlation():
    results = perpetrators_casualties_correlation_repo()
    buf = perpetrators_casualties_correlation_service(results)
    return Response(buf.getvalue(), mimetype='image/png')

#10
@stats_blueprint.route('/events_casualties_correlation')
def events_casualties_correlation():
    region_name = request.args.get('region', type=str)
    results = events_casualties_correlation_repo(region_name)
    buf = events_casualties_correlation_service(results, region_name)
    return Response(buf.getvalue(), mimetype='image/png')
