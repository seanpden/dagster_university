from dagster import AssetSelection, define_asset_job
from ..partitions import monthly_partitions, weekly_partitions


trips_by_week = AssetSelection.keys(["trips_by_week"])
adhoc_request = AssetSelection.keys(["adhoc_request"])

adhoc_request_job = define_asset_job(
    name="adhoc_request_job",
    selection=adhoc_request,
)

trip_update_job = define_asset_job(
    name="trip_update_job",
    partitions_def=monthly_partitions,
    selection=AssetSelection.all() - trips_by_week - adhoc_request,
)

weekly_update_job = define_asset_job(
    name="weekly_update_job",
    partitions_def=weekly_partitions,
    selection=trips_by_week,
)
