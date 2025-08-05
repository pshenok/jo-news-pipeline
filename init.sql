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

-- Create table for press release summaries
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

CREATE INDEX idx_press_release_id ON raw_data.press_release_summary(press_release_id);
