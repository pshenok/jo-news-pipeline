from dagster import (
    Definitions, 
    load_assets_from_modules,
    define_asset_job,
    AssetSelection,
    ScheduleDefinition,
    DefaultScheduleStatus
)

from src import assets
from src.resources.database import PostgresResource
from src.resources.scraper import ScraperResource
from src.resources.llm import LLMResource

# Load all assets
all_assets = load_assets_from_modules([assets])

# Define jobs for assets using proper selection
all_assets_job = define_asset_job(
    name="all_assets_job",
    description="Job to run all assets (scrape and summarize)"
)

# Schedule to run every 15 minutes
press_releases_schedule = ScheduleDefinition(
    name="press_releases_15min_schedule",
    cron_schedule="*/15 * * * *",  # Every 15 minutes
    job_name="all_assets_job",  # Reference job by name
    description="Run press release pipeline every 15 minutes",
    default_status=DefaultScheduleStatus.RUNNING,
    tags={
        "frequency": "15min",
        "pipeline": "press_releases",
        "auto_materialize": "true"
    }
)

# Alternative schedule - runs only during business hours
business_hours_schedule = ScheduleDefinition(
    name="press_releases_business_hours",
    cron_schedule="*/15 9-17 * * 1-5",  # Every 15 min, 9am-5pm, Mon-Fri
    job_name="all_assets_job",
    description="Run press release pipeline every 15 minutes during business hours",
    default_status=DefaultScheduleStatus.STOPPED,  # Not running by default
    tags={
        "frequency": "15min",
        "pipeline": "press_releases",
        "business_hours_only": "true"
    }
)

defs = Definitions(
    assets=all_assets,
    resources={
        "postgres": PostgresResource(),
        "scraper": ScraperResource(),
        "llm": LLMResource(),
    },
    jobs=[all_assets_job],
    schedules=[press_releases_schedule, business_hours_schedule]
)
