#!/bin/bash
# add-scraper-limit.sh - Add SCRAPER_LIMIT to .env configuration

cd jo-news-pipeline

echo "🔧 Adding SCRAPER_LIMIT configuration..."

# Update .env.example
cat >> .env.example << 'EOF'

# Scraper Configuration
SCRAPER_LIMIT=10  # Number of press releases to scrape per run
EOF

# Update .env if SCRAPER_LIMIT not already present
if ! grep -q "SCRAPER_LIMIT" .env; then
    echo -e "\n# Scraper Configuration" >> .env
    echo "SCRAPER_LIMIT=10  # Number of press releases to scrape per run" >> .env
    echo "✓ Added SCRAPER_LIMIT=10 to .env"
else
    echo "✓ SCRAPER_LIMIT already exists in .env"
fi

# Update the asset to use environment variable
cat > src/assets/scraper.py << 'EOF'
import os
import json
import hashlib
from datetime import datetime
from dagster import asset, AssetExecutionContext, MaterializeResult

@asset(
    required_resource_keys={"postgres", "scraper"}
)
def raw_press_releases(context: AssetExecutionContext) -> MaterializeResult:
    """Scrape SEC press releases and store in PostgreSQL."""
    
    postgres = context.resources.postgres
    scraper = context.resources.scraper
    
    # Get limit from environment variable
    scraper_limit = int(os.getenv("SCRAPER_LIMIT", "10"))
    context.log.info(f"Scraper limit set to: {scraper_limit}")
    
    # First, ensure table exists
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE SCHEMA IF NOT EXISTS raw_data;
                CREATE TABLE IF NOT EXISTS raw_data.press_releases (
                    id SERIAL PRIMARY KEY,
                    url VARCHAR(500) UNIQUE NOT NULL,
                    url_hash VARCHAR(64) NOT NULL,
                    title TEXT,
                    content TEXT,
                    published_at TIMESTAMP,
                    raw_response JSONB,
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
    
    # Get URLs with configurable limit
    urls = scraper.get_sec_urls(limit=scraper_limit)
    context.log.info(f"Found {len(urls)} URLs to process")
    
    if not urls:
        return MaterializeResult(
            metadata={"message": "No URLs found"}
        )
    
    # Check existing URLs
    existing = set()
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT url_hash FROM raw_data.press_releases WHERE url_hash = ANY(%s)",
                ([hashlib.sha256(url.encode()).hexdigest() for url in urls],)
            )
            existing = {row[0] for row in cursor.fetchall()}
    
    new_urls = [url for url in urls if hashlib.sha256(url.encode()).hexdigest() not in existing]
    context.log.info(f"Found {len(new_urls)} new URLs to scrape")
    
    # Scrape and save
    scraped = 0
    errors = 0
    
    for url in new_urls:
        try:
            result = scraper.scrape_url(url)
            
            if result['success']:
                parsed = scraper.parse_content(result['content'], url)
                
                with postgres.get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO raw_data.press_releases 
                            (url, url_hash, title, content, raw_response)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (url) DO NOTHING
                            RETURNING id
                        """, (
                            url,
                            result['url_hash'],
                            parsed['title'][:500] if parsed['title'] else 'No title',
                            parsed['content'][:5000] if parsed['content'] else 'No content',
                            json.dumps({
                                'url': url,
                                'scraped_at': result.get('scraped_at'),
                                'title': parsed['title'][:100] if parsed['title'] else None
                            })
                        ))
                        
                        inserted_id = cursor.fetchone()
                        if inserted_id:
                            context.log.info(f"✓ Inserted record ID: {inserted_id[0]} for {url}")
                            scraped += 1
                        else:
                            context.log.info(f"! Skipped (already exists): {url}")
            else:
                errors += 1
                context.log.error(f"✗ Failed to scrape {url}: {result.get('error')}")
                
        except Exception as e:
            errors += 1
            context.log.error(f"✗ Exception for {url}: {str(e)}")
    
    # Final count
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM raw_data.press_releases")
            total_count = cursor.fetchone()[0]
            context.log.info(f"Total records in database: {total_count}")
    
    return MaterializeResult(
        metadata={
            "scraper_limit": scraper_limit,
            "total_urls": len(urls),
            "new_urls": len(new_urls),
            "scraped": scraped,
            "errors": errors,
            "total_in_db": total_count
        }
    )
EOF

# Update docker-compose to pass the new environment variable
cat > docker-compose.yml << 'EOF'
version: '3.8'

services:
  postgres:
    image: postgres:17-alpine
    container_name: jo-news-postgres
    env_file:
      - .env
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    ports:
      - "${POSTGRES_PORT}:5432"
    volumes:
      - ./postgres_data:/var/lib/postgresql/data
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "${POSTGRES_USER}", "-d", "${POSTGRES_DB}"]
      interval: 5s
      timeout: 5s
      retries: 5

  dagster:
    build: .
    container_name: jo-news-dagster
    depends_on:
      postgres:
        condition: service_healthy
    env_file:
      - .env
    environment:
      POSTGRES_HOST: postgres
      POSTGRES_PORT: 5432
      SCRAPER_LIMIT: ${SCRAPER_LIMIT}
    ports:
      - "${DAGSTER_PORT}:3000"
    volumes:
      - ./dagster_home:/opt/dagster/home
      - ./src:/app/src
    command: ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-m", "src.definitions"]
EOF

echo -e "\n🔄 Restarting Dagster to apply changes..."
docker-compose restart dagster

echo -e "\n✅ Done! You can now control the scraper limit via .env"
echo "Current SCRAPER_LIMIT value:"
grep SCRAPER_LIMIT .env

echo -e "\nTo change the limit:"
echo "1. Edit .env file and change SCRAPER_LIMIT value"
echo "2. Restart Dagster: docker-compose restart dagster"
echo "3. Run the asset again"