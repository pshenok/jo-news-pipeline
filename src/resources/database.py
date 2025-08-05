import os
from contextlib import contextmanager
from typing import Iterator

import psycopg2
from dagster import ConfigurableResource
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


class PostgresResource(ConfigurableResource):
    @contextmanager
    def get_connection(self) -> Iterator[psycopg2.extensions.connection]:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=5432,
            database=os.getenv("POSTGRES_DB", "news_pipeline"),
            user=os.getenv("POSTGRES_USER", "dagster"),
            password=os.getenv("POSTGRES_PASSWORD", "dagster")
        )
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
