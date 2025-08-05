import os
import json
import hashlib
from datetime import datetime
from dagster import asset, AssetExecutionContext, MaterializeResult


@asset(
    required_resource_keys={"postgres", "scraper"}
)
def raw_press_releases(context: AssetExecutionContext) -> MaterializeResult:
    postgres = context.resources.postgres
    scraper = context.resources.scraper
    
    scraper_limit = int(os.getenv("SCRAPER_LIMIT", "10"))
    context.log.info(f"Scraper limit set to: {scraper_limit}")
    
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
    
    urls = scraper.get_sec_urls(limit=scraper_limit)
    context.log.info(f"Found {len(urls)} URLs to process")
    
    if not urls:
        return MaterializeResult(
            metadata={"message": "No URLs found"}
        )
    
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
    
    scraped = 0
    errors = 0
    
    for url in new_urls:
        try:
            result = scraper.scrape_url(url)
            
            if result['success']:
                parsed = scraper.parse_content(result['content'], url)
                
                # Extract published date if available
                published_at = parsed.get('published_at')
                
                with postgres.get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO raw_data.press_releases 
                            (url, url_hash, title, content, published_at, raw_response)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (url) DO NOTHING
                            RETURNING id
                        """, (
                            url,
                            result['url_hash'],
                            parsed['title'][:500] if parsed['title'] else 'No title',
                            parsed['content'][:5000] if parsed['content'] else 'No content',
                            published_at,  # This can be None if not found
                            json.dumps({
                                'url': url,
                                'scraped_at': result.get('scraped_at'),
                                'title': parsed['title'][:100] if parsed['title'] else None,
                                'published_at': published_at.isoformat() if published_at else None
                            })
                        ))
                        
                        inserted_id = cursor.fetchone()
                        if inserted_id:
                            scraped += 1
                            context.log.info(f"✓ Scraped: {parsed['title'][:80]}...")
            else:
                errors += 1
                context.log.error(f"Failed to scrape {url}: {result.get('error')}")
                
        except Exception as e:
            errors += 1
            context.log.error(f"Exception for {url}: {str(e)}")
    
    # Get some statistics
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM raw_data.press_releases")
            total_count = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT COUNT(*) FROM raw_data.press_releases 
                WHERE published_at >= NOW() - INTERVAL '30 days'
            """)
            recent_count = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT url, title, published_at 
                FROM raw_data.press_releases 
                ORDER BY published_at DESC NULLS LAST 
                LIMIT 5
            """)
            latest = cursor.fetchall()
            
            context.log.info("Latest press releases in DB:")
            for url, title, pub_date in latest:
                date_str = pub_date.strftime("%Y-%m-%d") if pub_date else "Unknown"
                context.log.info(f"  - {date_str}: {title[:60]}...")
    
    return MaterializeResult(
        metadata={
            "scraper_limit": scraper_limit,
            "total_urls": len(urls),
            "new_urls": len(new_urls),
            "scraped": scraped,
            "errors": errors,
            "total_in_db": total_count,
            "recent_releases": recent_count,
            "success_rate": f"{(scraped/len(new_urls)*100):.1f}%" if new_urls else "N/A"
        }
    )
