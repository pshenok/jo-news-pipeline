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
