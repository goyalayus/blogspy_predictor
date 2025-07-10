-- src/db/schema.sql

-- Drop existing types and tables if you are starting fresh
-- DROP TABLE IF EXISTS url_edges;
-- DROP TABLE IF EXISTS urls;
-- DROP TYPE IF EXISTS crawl_status;
-- DROP TYPE IF EXISTS rendering_type;

-- Define ENUM types for status and rendering
CREATE TYPE crawl_status AS ENUM (
    'pending_classification', 'classifying',
    'pending_crawl', 'crawling', 'completed',
    'irrelevant', 'failed'
);

CREATE TYPE rendering_type AS ENUM (
    'SSR', 'CSR', 'UNKNOWN'
);

-- Main table for storing URLs and their metadata (SIMPLIFIED)
CREATE TABLE urls (
    id             SERIAL PRIMARY KEY,
    url            TEXT           NOT NULL UNIQUE,
    netloc         TEXT           NOT NULL,
    status         crawl_status   NOT NULL DEFAULT 'pending_classification',
    rendering      rendering_type NOT NULL DEFAULT 'UNKNOWN',
    content        TEXT,
    title          TEXT,
    description    TEXT,
    error_message  TEXT,
    processed_at   TIMESTAMPTZ,
    locked_at      TIMESTAMPTZ, -- ADDED FOR FAULT TOLERANCE
    created_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_urls_status ON urls (status) WHERE status IN ('pending_classification', 'pending_crawl');
CREATE INDEX idx_urls_netloc_status ON urls (netloc, status);

-- Edge table for linking URLs
CREATE TABLE url_edges (
    source_url_id INTEGER NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
    dest_url_id   INTEGER NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
    PRIMARY KEY (source_url_id, dest_url_id)
);

CREATE INDEX idx_url_edges_dest ON url_edges (dest_url_id);
