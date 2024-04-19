from dagster import ScheduleDefinition
from ..jobs import trip_update_job, weekly_update_job


trip_update_schedule = ScheduleDefinition(
    cron_schedule="0 0 5 * *", job=trip_update_job
)

weekly_update_schedule = ScheduleDefinition(
    cron_schedule="0 0 * * 1",
    job=weekly_update_job,
)
