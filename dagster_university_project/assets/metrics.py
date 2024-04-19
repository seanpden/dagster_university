from dagster import asset
from dagster_duckdb import DuckDBResource

import plotly.express as px
import plotly.io as pio
import geopandas as gpd


from . import constants
from ..partitions import weekly_partitions


@asset(deps=["taxi_trips", "taxi_zones"])
def manhattan_stats(database: DuckDBResource):
    """
    GeoJSON file for manhattan stats.
    """
    sql_query = """
    select 
        zones.zone,
        zones.borough,
        zones.geometry,
        count(1) as num_trips,
    from trips
    left join zones on trips.pickup_zone_id = zones.zone_id
    where borough = 'Manhattan' and geometry is not null
    group by zone, borough, geometry
    """
    with database.get_connection() as conn:
        trips_by_zone = conn.execute(sql_query).fetch_df()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, "w") as f:
        f.write(trips_by_zone.to_json())


@asset(
    deps=["manhattan_stats"],
)
def manhattan_map():
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig = px.choropleth_mapbox(
        trips_by_zone,
        geojson=trips_by_zone.geometry.__geo_interface__,
        locations=trips_by_zone.index,
        color="num_trips",
        color_continuous_scale="Plasma",
        mapbox_style="carto-positron",
        center={"lat": 40.758, "lon": -73.985},
        zoom=11,
        opacity=0.7,
        labels={"num_trips": "Number of Trips"},
    )

    pio.write_image(fig, constants.MANHATTAN_MAP_FILE_PATH)


@asset(partitions_def=weekly_partitions, deps=["taxi_trips"])
def trips_by_week(context, database: DuckDBResource):
    """
    CSV file for trips by week.
    """

    period_to_fetch = context.asset_partition_key_for_output()

    sql_query = f"""
    select
        date_trunc('week', dropoff_datetime::DATE) as period,
        count(1) as num_trips,
        sum(passenger_count) as passenger_count,
        sum(total_amount::DECIMAL(10, 2)) as total_amount,
        sum(trip_distance::DECIMAL(10, 2)) as trip_distance,
    from trips
    where period >= '{period_to_fetch}' and period < '{period_to_fetch}'::date + interval '1 week'
    group by period
    order by period
    """

    with database.get_connection() as conn:
        trips_by_week = conn.execute(sql_query).fetch_df()

    trips_by_week.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)
