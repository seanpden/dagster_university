from dagster import Config, asset
from dagster_duckdb import DuckDBResource
from . import constants

import plotly.express as px
import plotly.io as pio


class AdHocRequestConfig(Config):
    filename: str
    borough: str
    start_date: str
    end_date: str


@asset(deps=["taxi_zones", "taxi_trips"])
def adhoc_request(config: AdHocRequestConfig, database: DuckDBResource):
    req_filename = config.filename.split(".")[0]  # strip extension from filename
    file_path = constants.REQUEST_DESTINATION_TEMPLATE_FILE_PATH.format(req_filename)

    sql_query = f"""
    SELECT 
        date_part('hour', pickup_datetime) AS 'hour_of_day',
        date_part('dayofweek', pickup_datetime) AS 'day_of_week',
        dayname(pickup_datetime) AS 'day_of_week_name',
        count(*) AS 'num_trips'
    FROM trips
    WHERE
        pickup_zone_id IN 
            (
                SELECT zone_id 
                FROM zones 
                WHERE borough = '{config.borough}'
            ) AND
        pickup_datetime >= '{config.start_date}' AND
        pickup_datetime < '{config.end_date}'
    GROUP BY
        hour_of_day, day_of_week, day_of_week_name
    ORDER BY
        day_of_week_name
    """

    with database.get_connection() as conn:
        results = conn.execute(sql_query).fetch_df()

    fig = px.bar(
        results,
        x="hour_of_day",
        y="num_trips",
        color="day_of_week",
        barmode="stack",
        title=f"Number of trips by hour of day in {config.borough}, from {config.start_date} to {config.end_date}",
        labels={
            "hour_of_day": "Hour of Day",
            "day_of_week": "Day of Week",
            "num_trips": "Number of Trips",
        },
    )

    pio.write_image(fig, file_path)
