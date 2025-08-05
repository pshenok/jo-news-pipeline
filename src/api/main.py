import os
from datetime import datetime
from typing import List, Optional
from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI(title="Press Releases API", version="1.0.0")


class PressRelease(BaseModel):
    title: str
    date: str
    url: str
    summary: Optional[str] = None


class ReleasesResponse(BaseModel):
    releases: List[PressRelease]
    total: int
    limit: int


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "news_pipeline"),
        user=os.getenv("POSTGRES_USER", "dagster"),
        password=os.getenv("POSTGRES_PASSWORD", "dagster")
    )


@app.get("/")
def read_root():
    return {"status": "ok", "service": "Press Releases API"}


@app.get("/releases", response_model=ReleasesResponse)
def get_releases(limit: int = Query(default=20, ge=1, le=100)):
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        pr.title,
                        pr.published_at,
                        pr.url,
                        prs.summary,
                        prs.bullet_points
                    FROM raw_data.press_releases pr
                    LEFT JOIN raw_data.press_release_summary prs 
                        ON pr.id = prs.press_release_id
                    ORDER BY pr.published_at DESC NULLS LAST, pr.created_at DESC
                    LIMIT %s
                """, (limit,))
                
                rows = cursor.fetchall()
                
                cursor.execute("SELECT COUNT(*) FROM raw_data.press_releases")
                total = cursor.fetchone()['count']
                
                releases = []
                for row in rows:
                    date_str = "Unknown"
                    if row['published_at']:
                        date_str = row['published_at'].strftime("%Y-%m-%d")
                    
                    summary = row['summary']
                    if not summary and row['bullet_points']:
                        import json
                        try:
                            bullets = json.loads(row['bullet_points'])
                            summary = ' • '.join(bullets)
                        except:
                            summary = "Summary not available"
                    elif not summary:
                        summary = "Summary not available"
                    
                    releases.append(PressRelease(
                        title=row['title'] or "No title",
                        date=date_str,
                        url=row['url'],
                        summary=summary
                    ))
                
                return ReleasesResponse(
                    releases=releases,
                    total=total,
                    limit=limit
                )
                
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/stats")
def get_stats():
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_releases,
                        COUNT(DISTINCT prs.id) as total_summarized,
                        MIN(pr.published_at) as oldest_release,
                        MAX(pr.published_at) as newest_release,
                        MAX(pr.scraped_at) as last_scraped
                    FROM raw_data.press_releases pr
                    LEFT JOIN raw_data.press_release_summary prs 
                        ON pr.id = prs.press_release_id
                """)
                
                stats = cursor.fetchone()
                
                return {
                    "total_releases": stats['total_releases'],
                    "total_summarized": stats['total_summarized'],
                    "oldest_release": stats['oldest_release'].isoformat() if stats['oldest_release'] else None,
                    "newest_release": stats['newest_release'].isoformat() if stats['newest_release'] else None,
                    "last_scraped": stats['last_scraped'].isoformat() if stats['last_scraped'] else None,
                    "summary_percentage": round((stats['total_summarized'] / stats['total_releases'] * 100), 2) if stats['total_releases'] > 0 else 0
                }
                
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
