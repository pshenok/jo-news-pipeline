import os
import psycopg2
from contextlib import contextmanager
from dagster import ConfigurableResource

class PostgresResource(ConfigurableResource):
    """PostgreSQL database resource."""
    
    @contextmanager
    def get_connection(self):
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
