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

CREATE INDEX idx_url_hash ON raw_data.press_releases(url_hash);
CREATE INDEX idx_created_at ON raw_data.press_releases(created_at DESC);
