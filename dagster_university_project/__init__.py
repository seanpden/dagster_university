# fmt: off
from dagster import Definitions, load_assets_from_modules

from .assets import metrics, trips, request
from .resources import database_resource
from .jobs import trip_update_job, weekly_update_job, adhoc_request_job
from .sensors import adhoc_request_sensor
from .schedules import trip_update_schedule, weekly_update_schedule

trip_assets = load_assets_from_modules([trips], group_name="trips")
metric_assets = load_assets_from_modules([metrics], group_name="metrics")
request_assets = load_assets_from_modules([request], group_name="request")
all_assets = [*trip_assets, *metric_assets, *request_assets]

all_resources = {
    "database": database_resource
        }

all_jobs = [trip_update_job, weekly_update_job, adhoc_request_job]

all_sensors = [adhoc_request_sensor]

all_schedules = [trip_update_schedule, weekly_update_schedule]

defs = Definitions(
    assets=all_assets,
    resources=all_resources, 
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=all_sensors,
)
