import io
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import folium
import seaborn as sns
from folium import plugins

def create_map(center=None, zoom=2):
    if center is None:
        center = [0, 0]
    return folium.Map(
        location=center,
        zoom_start=zoom,
        tiles='CartoDB positron'
    )

def deadliest_attacks_service(results):
    df = pd.DataFrame(results, columns=['attack_type', 'casualty_score'])
    plt.figure(figsize=(10, 6))
    plt.bar(df['attack_type'], df['casualty_score'])
    plt.xticks(rotation=45, ha='right')
    plt.title('Deadliest Attack Types')
    plt.ylabel('Casualty Score (Killed×2 + Wounded)')
    plt.xlabel('Attack Type')
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=300, bbox_inches='tight')
    buf.seek(0)
    plt.close()
    return buf

def casualties_by_region_service(results):
    m = create_map()
    for region, count, score, lat, lon in results:
        if lat and lon:
            avg_casualties = score / count if count > 0 else 0
            folium.Circle(
                location=[lat, lon],
                radius=avg_casualties * 100,
                color='red',
                fill=True,
                popup=f"{region}<br>Avg Casualties: {avg_casualties:.2f}<br>Total Events: {count}"
            ).add_to(m)
    buf = io.BytesIO()
    m.save(buf, close_file=False)
    return buf

def top_casualty_groups_service(results):
    plt.figure(figsize=(10, 6))
    plt.bar([r[0] for r in results], [r[1] for r in results])
    plt.title('Top 5 Most Lethal Terrorist Groups')
    plt.xlabel('Group Name')
    plt.ylabel('Total Casualties')
    plt.xticks(rotation=45)
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()
    return buf

def attack_target_correlation_service(results):
    df = pd.DataFrame(results, columns=['attack_type', 'target_type', 'event_count'])
    correlation_matrix = df.pivot_table(
        values='event_count',
        index='attack_type',
        columns='target_type'
    ).corr()
    plt.figure(figsize=(10, 8))
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm')
    plt.title('Attack-Target Type Correlation')
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()
    return buf

def attack_trends_service(annual_trends,monthly_trends,year):
    plt.figure(figsize=(15, 10))
    plt.subplot(2, 1, 1)
    plt.bar([str(trend.year) for trend in annual_trends],
            [trend.attack_count for trend in annual_trends])
    plt.title('Annual Attack Trends')
    plt.xlabel('Year')
    plt.ylabel('Number of Attacks')
    plt.xticks(rotation=45)
    plt.subplot(2, 1, 2)
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
              'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    month_counts = [next((trend.attack_count for trend in monthly_trends if trend.month == i + 1), 0)
                    for i in range(12)]
    plt.bar(months, month_counts)
    plt.title(f'Monthly Attack Trends in {year}')
    plt.xlabel('Month')
    plt.ylabel('Number of Attacks')
    plt.xticks(rotation=45)
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()
    return buf

def attack_change_by_region_service(df,top_n):
    df['percent_change'] = ((df['current_attacks'] - df['previous_attacks']) / df['previous_attacks'] * 100).fillna(0)
    top_regions = df.groupby('region')['percent_change'].mean().abs().nlargest(top_n)
    plt.figure(figsize=(12, 6))
    plt.bar(top_regions.index, top_regions.values)
    plt.title(f'Top {top_n} Regions - Attack Percentage Change')
    plt.xlabel('Region')
    plt.ylabel('Average Percentage Change')
    plt.xticks(rotation=45)
    for i, v in enumerate(top_regions.values):
        plt.text(i, v, f'{v:.2f}%', ha='center', va='bottom')
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()
    return buf

def terror_heatmap_service(locations,current_year,time_period,region_filter):
    m = create_map()

    if time_period in ['3_years', '5_years']:
        years_data = {}
        for year in range(current_year - (3 if time_period == '3_years' else 5), current_year + 1):
            year_locations = [
                [loc.latitude, loc.longitude]
                for loc in locations if loc.year == year
            ]
            if year_locations:
                years_data[year] = year_locations

        if years_data:
            plugins.HeatMapWithTime(
                [list(years_data.values())],
                index=list(years_data.keys()),
                auto_play=True,
                max_opacity=0.8
            ).add_to(m)
    else:
        heat_data = [[loc.latitude, loc.longitude] for loc in locations]
        plugins.HeatMap(heat_data,
                        name='Terror Hotspots',
                        max_opacity=0.8
                        ).add_to(m)

    folium.LayerControl().add_to(m)
    stats_html = f"""
        <div style='width: 300px; background: white; padding: 10px;'>
        <h4>Terror Hotspots Analysis</h4>
        <p>Total Events: {len(locations)}</p>
        <p>Time Period: {time_period}</p>
        {'<p>Region: ' + region_filter + '</p>' if region_filter else ''}
        </div>
        """
    m.get_root().html.add_child(folium.Element(stats_html))
    buf = io.BytesIO()
    m.save(buf, close_file=False)
    return buf

def active_groups_heatmap_service(results, region_filter):
    m = create_map()

    for result in results:
        if len(result) == 5:
            group, region, count, lat, lon = result
            popup_text = f"""
                <b>{group}</b><br>
                Region: {region}<br>
                Attacks: {count}
            """
        else:
            group, count, lat, lon = result
            popup_text = f"""
                <b>{group}</b><br>
                Attacks: {count}<br>
                {'Region: ' + region_filter if region_filter else ''}
            """

        if lat and lon:
            marker = folium.Marker(
                location=[lat, lon],
                popup=popup_text,
                tooltip=group,
                icon=folium.Icon(color='red', icon='info-sign')
            )
            marker.add_to(m)

    summary_html = f"""
    <div style='position: fixed; 
                bottom: 50px; 
                left: 50px; 
                z-index: 1000;
                background-color: white;
                padding: 10px;
                border: 2px solid #ccc;
                border-radius: 5px;'>
        <h4>Active Groups Analysis</h4>
        <p>Showing top 5 most active groups{' in ' + region_filter if region_filter else ' per region'}</p>
        <p>Total groups shown: {len(results)}</p>
    </div>
    """
    m.get_root().html.add_child(folium.Element(summary_html))

    buf = io.BytesIO()
    m.save(buf, close_file=False)
    return buf

def perpetrators_casualties_correlation_service(results):
    df = pd.DataFrame(results, columns=['event_id', 'perpetrator_count', 'total_casualties'])
    df = df[(df['perpetrator_count'] > 0) & (df['total_casualties'] > 0)]
    if len(df) < 2:
        plt.figure(figsize=(10, 6))
        plt.text(0.5, 0.5, 'Insufficient data for correlation analysis',
                 horizontalalignment='center', verticalalignment='center')
        plt.title('Perpetrators vs Casualties Correlation')
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        plt.close()
        return buf
    try:
        correlation = df['perpetrator_count'].corr(df['total_casualties'])
    except Exception:
        correlation = 0
    plt.figure(figsize=(12, 6))
    plt.scatter(df['perpetrator_count'], df['total_casualties'],
                alpha=0.6,
                c=df['perpetrator_count'],
                cmap='viridis')
    try:
        m, b = np.polyfit(df['perpetrator_count'], df['total_casualties'], 1)
        x_line = np.linspace(df['perpetrator_count'].min(), df['perpetrator_count'].max(), 100)
        plt.plot(x_line, m * x_line + b, color='red', linestyle='--', label='Trend Line')
    except Exception:
        print("Could not fit regression line")
    plt.colorbar(label='Perpetrator Count')
    plt.title(f'Perpetrators vs Casualties Correlation\nCorrelation Coefficient: {correlation:.4f}')
    plt.xlabel('Number of Perpetrators')
    plt.ylabel('Total Casualties')
    plt.legend()
    stats_text = f"""
        Correlation: {correlation:.4f}
        Data Points: {len(df)}
        Perpetrators (Avg): {df['perpetrator_count'].mean():.2f}
        Casualties (Avg): {df['total_casualties'].mean():.2f}
        """
    plt.annotate(stats_text,
                 xy=(0.05, 0.95),
                 xycoords='axes fraction',
                 bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8),
                 verticalalignment='top')
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=300, bbox_inches='tight')
    buf.seek(0)
    plt.close()
    return buf

def events_casualties_correlation_service(results,region_name):
    df = pd.DataFrame(results, columns=['region', 'event_count', 'total_casualties'])
    correlation = df['event_count'].corr(df['total_casualties'])
    plt.figure(figsize=(12, 8))
    scatter = plt.scatter(
        df['event_count'],
        df['total_casualties'],
        c=df['total_casualties'],
        s=df['event_count'],
        alpha=0.6,
        cmap='viridis'
    )
    plt.colorbar(scatter, label='Total Casualties')
    m, b = np.polyfit(df['event_count'], df['total_casualties'], 1)
    plt.plot(df['event_count'], m * df['event_count'] + b, color='red', linestyle='--')
    plt.title(f'Event Count vs Casualties Correlation\nby {"Specific Region" if region_name else "All Regions"}')
    plt.xlabel('Number of Events')
    plt.ylabel('Total Casualties')
    for _, row in df.iterrows():
        plt.annotate(row['region'],
                     (row['event_count'], row['total_casualties']),
                     xytext=(5, 5),
                     textcoords='offset points',
                     fontsize=8)
    plt.annotate(f'Correlation: {correlation:.4f}',
                 xy=(0.05, 0.95),
                 xycoords='axes fraction',
                 bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8))
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()
    return buf