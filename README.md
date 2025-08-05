# jo-news-pipeline

SEC press release pipeline with automated scraping, summarization, and API serving.

## Architecture

- **Dagster**: Orchestrates data pipeline with 15-minute scheduling
- **PostgreSQL 17**: Stores raw press releases and summaries
- **Ollama**: LLM service for text summarization (qwen2.5:0.5b model)
- **ScrapingBee**: Web scraping service
- **FastAPI**: REST API for data access

## Quick Start

### Prerequisites

- Docker and Docker Compose
- ScrapingBee API key (free tier available at https://www.scrapingbee.com/)

### Installation

1. Clone repository and navigate to project directory
```bash
cd jo-news-pipeline
```

2. Configure environment
```bash
cp .env.example .env
# Edit .env and add your SCRAPER_API_KEY
```

3. Start services
```bash
docker-compose up -d
```

4. Access interfaces
- Dagster UI: http://localhost:3000
- API Documentation: http://localhost:8000/docs
- API Endpoint: http://localhost:8000/releases?limit=20

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SCRAPER_API_KEY` | ScrapingBee API key | Required |
| `POSTGRES_USER` | Database user | dagster |
| `POSTGRES_PASSWORD` | Database password | dagster |
| `POSTGRES_DB` | Database name | news_pipeline |
| `POSTGRES_PORT` | Database port | 5432 |
| `DAGSTER_PORT` | Dagster UI port | 3000 |
| `SCRAPER_LIMIT` | Press releases per run | 10 |
| `LLM_MODEL` | Ollama model | qwen2.5:0.5b |

## Pipeline Components

### Assets

1. **raw_press_releases**: Scrapes SEC press releases, stores in PostgreSQL
   - Deduplicates by URL hash
   - Configurable limit via SCRAPER_LIMIT

2. **press_release_summary**: Generates 3-bullet summaries using LLM
   - Processes all unsummarized releases
   - 50-word limit per summary

### Schedule

Pipeline runs automatically every 15 minutes. View schedule status in Dagster UI under "Schedules" tab.

### API Endpoints

- `GET /` - Health check
- `GET /releases?limit=20` - Get press releases with summaries
- `GET /stats` - Pipeline statistics

## Database Schema

### raw_data.press_releases
- `id`: Primary key
- `url`: Press release URL (unique)
- `url_hash`: SHA256 hash for deduplication
- `title`: Article title
- `content`: Full text content
- `published_at`: Publication date
- `raw_response`: Original scrape response (JSONB)
- `scraped_at`: Scrape timestamp
- `created_at`: Record creation timestamp

### raw_data.press_release_summary
- `id`: Primary key
- `press_release_id`: Foreign key to press_releases
- `summary`: Formatted summary text
- `bullet_points`: Array of bullet points (JSONB)
- `word_count`: Total words in summary
- `model_used`: LLM model identifier
- `summarized_at`: Summary generation timestamp

## Testing

Run test suite:
```bash
docker exec jo-news-dagster pytest src/tests/ -v
```

Test categories:
- Database resource tests
- Scraper resource tests
- LLM resource tests
- Asset tests

## Database Access

PostgreSQL connection:
- Host: localhost
- Port: 5432
- Database: news_pipeline
- Username: dagster
- Password: dagster

Query examples:
```sql
-- View recent press releases
SELECT * FROM raw_data.press_releases ORDER BY created_at DESC LIMIT 10;

-- Check summary statistics
SELECT COUNT(*) as total, 
       COUNT(DISTINCT s.id) as summarized 
FROM raw_data.press_releases p
LEFT JOIN raw_data.press_release_summary s ON p.id = s.press_release_id;
```

## Monitoring

### Dagster UI
- View asset materializations
- Monitor run history
- Check schedule execution

### API Statistics
```bash
curl http://localhost:8000/stats
```

### Database Monitoring
```bash
docker exec jo-news-postgres psql -U dagster -d news_pipeline -c "SELECT * FROM raw_data.press_release_stats;"
```

## Troubleshooting

### Ollama Connection Issues
```bash
# Check Ollama status
curl http://localhost:11434/api/tags

# Restart Ollama
docker-compose restart ollama
```

### Dagster Issues
```bash
# View logs
docker logs jo-news-dagster --tail 50

# Restart Dagster
docker-compose restart dagster
```

### Database Issues
```bash
# Check PostgreSQL logs
docker logs jo-news-postgres --tail 50

# Connect to database
docker exec -it jo-news-postgres psql -U dagster -d news_pipeline
```

## Development

### Project Structure
```
jo-news-pipeline/
├── src/
│   ├── api/           # FastAPI application
│   ├── assets/        # Dagster assets
│   ├── resources/     # External resources
│   └── tests/         # Test suite
├── docker-compose.yml
├── requirements.txt
└── README.md
```

### Adding New Assets

1. Create asset in `src/assets/`
2. Import in `src/assets/__init__.py`
3. Restart Dagster: `docker-compose restart dagster`

### Modifying Schedule

Edit `src/definitions.py` to adjust schedule frequency or add new schedules.
