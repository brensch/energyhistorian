from dagster import Definitions, ScheduleDefinition, define_asset_job, load_assets_from_modules

from . import assets


nemweb_assets = load_assets_from_modules([assets])
nemweb_job = define_asset_job(name="nemweb_job", selection=nemweb_assets)

defs = Definitions(
    assets=nemweb_assets,
    schedules=[
        ScheduleDefinition(
            job=nemweb_job,
            cron_schedule="*/5 * * * *",
            name="nemweb_every_five_minutes",
        )
    ],
    jobs=[nemweb_job],
)
