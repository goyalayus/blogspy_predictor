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

-- MODIFIED: This is the "hot" table for job queueing. It's now much thinner.
CREATE TABLE urls (
    id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    netloc TEXT NOT NULL,
    status crawl_status NOT NULL DEFAULT 'pending_classification',
    rendering rendering_type,
    error_message TEXT,
    locked_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    pagerank_score REAL DEFAULT 1.0
    -- DELETED: title, description, content have been moved.
);

-- NEW: This is the "cold" table for storing "write-once" content.
-- It is UNLOGGED for maximum write performance, as the data is non-critical
-- and can be repopulated by re-crawling. It will be truncated on DB crash.
CREATE TABLE url_content (
    url_id BIGINT PRIMARY KEY REFERENCES urls(id) ON DELETE CASCADE,
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


CREATE INDEX idx_urls_pending_classification ON urls (id)
WHERE status = 'pending_classification';

CREATE INDEX idx_urls_pending_crawl ON urls (id)
WHERE status = 'pending_crawl';

CREATE TABLE system_counters (
    counter_name TEXT PRIMARY KEY,
    value BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ
);

INSERT INTO system_counters (counter_name) VALUES ('pending_urls_count');

CREATE TABLE netloc_counts (
    netloc TEXT PRIMARY KEY,
    url_count INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL
);
