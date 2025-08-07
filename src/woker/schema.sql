-- Enables the vector data type and functions.
-- This must be run by a database superuser one time.
CREATE EXTENSION IF NOT EXISTS vector;

-- ========= Custom Types =========

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

-- ========= Core Tables =========

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
);

CREATE TABLE url_content (
    url_id BIGINT PRIMARY KEY REFERENCES urls(id) ON DELETE CASCADE,
    title TEXT,
    description TEXT,
    content TEXT,
    -- This column will store pre-computed tsvector for full-text search
    search_vector tsvector,
    -- This column will store the 256-dimension vector embedding
    embedding vector(256)
);

CREATE TABLE url_edges (
    source_url_id BIGINT NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
    dest_url_id BIGINT NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
    PRIMARY KEY (source_url_id, dest_url_id)
);

-- ========= System & Helper Tables =========

CREATE TABLE system_counters (
    counter_name TEXT PRIMARY KEY,
    value BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ
);


-- ========= Full-Text Search Function & Trigger =========
-- This function and trigger automatically populates the `search_vector` column.
-- It assigns higher weight to the title (A) than to the description (B).
CREATE OR REPLACE FUNCTION url_content_search_vector_update() RETURNS trigger AS $$
BEGIN
    new.search_vector :=
        setweight(to_tsvector('english', coalesce(new.title, '')), 'A') ||
        setweight(to_tsvector('english', coalesce(new.description, '')), 'B');
    return new;
END
$$ LANGUAGE plpgsql;

-- The trigger executes the function before an insert or update on url_content.
CREATE TRIGGER tsvectorupdate BEFORE INSERT OR UPDATE
ON url_content FOR EACH ROW EXECUTE PROCEDURE url_content_search_vector_update();


-- ========= Indexes =========

-- Crawler-specific indexes
CREATE INDEX idx_urls_netloc ON urls (netloc);
CREATE INDEX idx_urls_pending_classification ON urls (id) WHERE status = 'pending_classification';
CREATE INDEX idx_urls_pending_crawl ON urls (id) WHERE status = 'pending_crawl';

-- Full-text search index (crucial for FTS performance)
CREATE INDEX idx_url_content_search_vector ON url_content USING GIN(search_vector);

-- Vector search index (crucial for vector similarity search performance)
-- Using HNSW with cosine distance for high-performance, accurate vector search.
CREATE INDEX idx_url_content_embedding ON url_content USING hnsw (embedding vector_cosine_ops);


-- ========= Initial Data =========
INSERT INTO system_counters (counter_name) VALUES ('pending_urls_count');
