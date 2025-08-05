import json
from dagster import asset, AssetExecutionContext, MaterializeResult

@asset(
    deps=["raw_press_releases"],
    required_resource_keys={"postgres", "llm"}
)
def press_release_summary(context: AssetExecutionContext) -> MaterializeResult:
    postgres = context.resources.postgres
    llm = context.resources.llm
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS raw_data.press_release_summary (
                    id SERIAL PRIMARY KEY,
                    press_release_id INTEGER REFERENCES raw_data.press_releases(id) UNIQUE,
                    summary TEXT NOT NULL,
                    bullet_points JSONB,
                    word_count INTEGER,
                    model_used VARCHAR(100),
                    summarized_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT pr.id, pr.title, pr.content 
                FROM raw_data.press_releases pr
                LEFT JOIN raw_data.press_release_summary prs 
                    ON pr.id = prs.press_release_id
                WHERE prs.id IS NULL
                ORDER BY pr.created_at DESC
            """)
            unsummarized = cursor.fetchall()
    
    total_to_process = len(unsummarized)
    context.log.info(f"Found {total_to_process} press releases to summarize")
    
    if total_to_process == 0:
        return MaterializeResult(
            metadata={
                "processed": 0,
                "summarized": 0,
                "errors": 0,
                "message": "All press releases already summarized"
            }
        )
    
    if not llm.test_connection():
        context.log.error("LLM service (Ollama) is not available")
        return MaterializeResult(
            metadata={
                "error": "LLM service not available",
                "processed": 0,
                "summarized": 0,
                "unsummarized_count": total_to_process
            }
        )
    
    summarized = 0
    errors = 0
    
    batch_size = 10
    for i in range(0, total_to_process, batch_size):
        batch = unsummarized[i:i + batch_size]
        batch_end = min(i + batch_size, total_to_process)
        context.log.info(f"Processing batch {i//batch_size + 1}: items {i+1}-{batch_end} of {total_to_process}")
        
        for release_id, title, content in batch:
            try:
                result = llm.summarize(content or "", title or "")
                
                with postgres.get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO raw_data.press_release_summary 
                            (press_release_id, summary, bullet_points, word_count, model_used)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (press_release_id) DO NOTHING
                            RETURNING id
                        """, (
                            release_id,
                            result['summary'],
                            json.dumps(result['bullet_points']),
                            result['word_count'],
                            result['model_used']
                        ))
                        
                        if cursor.fetchone():
                            summarized += 1
                        
            except Exception as e:
                errors += 1
                context.log.error(f"Error summarizing release ID {release_id}: {str(e)}")
        
        progress_pct = round((i + len(batch)) / total_to_process * 100, 1)
        context.log.info(f"Progress: {progress_pct}% complete ({summarized} summarized, {errors} errors)")
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM raw_data.press_release_summary")
            total_summaries = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT COUNT(*) 
                FROM raw_data.press_releases pr
                LEFT JOIN raw_data.press_release_summary prs 
                    ON pr.id = prs.press_release_id
                WHERE prs.id IS NULL
            """)
            remaining_unsummarized = cursor.fetchone()[0]
    
    return MaterializeResult(
        metadata={
            "processed": total_to_process,
            "summarized": summarized,
            "errors": errors,
            "total_summaries_in_db": total_summaries,
            "remaining_unsummarized": remaining_unsummarized,
            "success_rate": f"{round(summarized/total_to_process*100, 1)}%" if total_to_process > 0 else "N/A"
        }
    )
