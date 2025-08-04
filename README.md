# jo-news-pipeline

Simple news scraping pipeline with Dagster.

## Quick Start

1. Copy `.env.example` to `.env` and add your ScrapingBee API key
2. Run `docker-compose up -d`
3. Open http://localhost:3000
4. Click "Materialize all" to run the pipeline

## Check Data

```bash
docker exec -it jo-news-pipeline-postgres-1 psql -U dagster -d news_pipeline -c "SELECT * FROM raw_data.press_releases LIMIT 5;"
```

## PostgreSQL GUI Connection

Connect using your favorite PostgreSQL client (pgAdmin, DBeaver, TablePlus, etc.):

- **Host**: localhost
- **Port**: 5432 (or value from POSTGRES_PORT in .env)
- **Database**: news_pipeline (or value from POSTGRES_DB in .env)
- **Username**: dagster (or value from POSTGRES_USER in .env)
- **Password**: dagster (or value from POSTGRES_PASSWORD in .env)
