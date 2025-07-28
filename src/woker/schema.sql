-- woker/schema.sql

CREATE TYPE crawl_status AS ENUM (
    'pending_classification',
    'pending_crawl',
    'classifying',
    'crawling',
    'completed',
    'failed',
    'irrelevant'
);

CREATE TYPE rendering_type AS ENUM (
    'SSR',
    'CSR'
);

CREATE TABLE urls (
    id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    netloc TEXT NOT NULL,
    status crawl_status NOT NULL DEFAULT 'pending_classification',
    rendering rendering_type,
    error_message TEXT,
    locked_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    title TEXT,
    description TEXT,
    content TEXT
);

CREATE TABLE url_edges (
    source_url_id BIGINT NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
    dest_url_id BIGINT NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
    PRIMARY KEY (source_url_id, dest_url_id)
);

CREATE INDEX idx_urls_netloc ON urls (netloc);

-- DROP THE OLD, INEFFICIENT INDEX
DROP INDEX IF EXISTS idx_urls_status;

-- CREATE NEW, HIGHLY EFFICIENT PARTIAL INDEXES FOR THE JOB QUEUE
-- This index is tiny and only contains rows waiting for classification.
CREATE INDEX idx_urls_pending_classification ON urls (id)
WHERE status = 'pending_classification';

-- This index is tiny and only contains rows waiting for crawling.
CREATE INDEX idx_urls_pending_crawl ON urls (id)
WHERE status = 'pending_crawl';


-- NEW TABLE FOR CACHING SYSTEM-WIDE COUNTERS
CREATE TABLE system_counters (
    counter_name TEXT PRIMARY KEY,
    value BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ
);

-- PRE-POPULATE THE COUNTER WE NEED
INSERT INTO system_counters (counter_name) VALUES ('pending_urls_count');
