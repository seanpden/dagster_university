import requests
from dagster_duckdb import DuckDBResource
from dagster import asset, MaterializeResult
import pandas as pd
from . import constants
from ..partitions import monthly_partitions


@asset(partitions_def=monthly_partitions, group_name="raw_files")
def taxi_trips_file(context):
    """
    Raw parquet files for the taxi trips dataset. Sourced from NYC Open Data portal.
    """
    partition_date_str = context.asset_partition_key_for_output()
    month_to_fetch = partition_date_str[:-3]
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    raw_trips = requests.get(url)
    if raw_trips.status_code != 200:
        raise Exception(f"Couldn't fetch data, status code: {raw_trips.status_code}")

    with open(
        constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb"
    ) as f:
        f.write(raw_trips.content)

    num_rows = len(
        pd.read_parquet(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch))
    )
    return MaterializeResult(
        metadata={"num_rows": num_rows},
    )


@asset(group_name="raw_files")
def taxi_zones_file():
    """
    Raw CSV file for the taxi zones dataset. Sourced from NYC Open Data portal.
    """
    url = (
        "https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD"
    )
    taxi_zones = requests.get(url)
    if taxi_zones.status_code != 200:
        raise Exception(f"Couldn't fetch data, status code: {taxi_zones.status_code}")

    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as f:
        f.write(taxi_zones.content)


@asset(
    partitions_def=monthly_partitions, deps=["taxi_trips_file"], group_name="ingested"
)
def taxi_trips(context, database: DuckDBResource):
    """
    Raw taxi trips dataset, loaded into a DuckDB database.
    """
    partition_date_str = context.asset_partition_key_for_output()
    month_to_fetch = partition_date_str[:-3]

    sql_query = f"""
      create table if not exists trips (
        vendor_id integer, pickup_zone_id integer, dropoff_zone_id integer,
        rate_code_id double, payment_type integer, dropoff_datetime timestamp,
        pickup_datetime timestamp, trip_distance double, passenger_count double,
        total_amount double, partition_date varchar
      );

      delete from trips where partition_date = '{month_to_fetch}';

      insert into trips
      select
        VendorID, PULocationID, DOLocationID, RatecodeID, payment_type, tpep_dropoff_datetime,
        tpep_pickup_datetime, trip_distance, passenger_count, total_amount, '{month_to_fetch}' as partition_date
      from '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}';
    """

    with database.get_connection() as conn:
        conn.execute(sql_query)


@asset(deps=["taxi_zones_file"], group_name="ingested")
def taxi_zones(database: DuckDBResource):
    """
    Raw taxi zones dataset, loaded into a DuckDB database.
    """
    sql_query = f"""
        create or replace table zones as (
            select
                LocationID as zone_id,
                zone as zone,
                borough as borough,
                the_geom as geometry
            from '{constants.TAXI_ZONES_FILE_PATH}'
        );
    """

    with database.get_connection() as conn:
        conn.execute(sql_query)
