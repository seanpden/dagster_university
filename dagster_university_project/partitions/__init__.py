from dagster import MonthlyPartitionsDefinition, WeeklyPartitionsDefinition
from ..assets import constants

START_DATE = constants.START_DATE
END_DATE = constants.END_DATE


monthly_partitions = MonthlyPartitionsDefinition(
    start_date=START_DATE, end_date=END_DATE
)

weekly_partitions = WeeklyPartitionsDefinition(start_date=START_DATE, end_date=END_DATE)
