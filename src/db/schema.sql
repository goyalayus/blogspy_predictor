CREATE TYPE crawl_status AS ENUM (
    'pending_classification',
    'classifying',
    'pending_crawl',
    'crawling',
    'completed',
    'irrelevant',
    'failed'
);

CREATE TABLE urls (
    id             SERIAL PRIMARY KEY,
    url            TEXT        NOT NULL UNIQUE,
    status         crawl_status NOT NULL DEFAULT 'pending_classification',
    content        TEXT,
    classification TEXT,
    confidence     REAL,
    error_message  TEXT,
    processed_at   TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_urls_status ON urls (status) WHERE status IN ('pending_classification', 'pending_crawl');
