from dagster import Definitions, load_assets_from_modules
from src import assets
from src.resources.database import PostgresResource
from src.resources.scraper import ScraperResource

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources={
        "postgres": PostgresResource(),
        "scraper": ScraperResource(),
    },
)
