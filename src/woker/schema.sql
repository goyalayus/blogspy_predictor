CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS plpgsql;

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
    processed_at TIMESTAMPTZ
);

CREATE TABLE url_content (
    url_id BIGINT PRIMARY KEY REFERENCES urls(id) ON DELETE CASCADE,
    title TEXT,
    description TEXT,
    content TEXT,
    search_vector tsvector
);

CREATE TABLE url_edges (
    source_url_id BIGINT NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
    dest_url_id BIGINT NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
    PRIMARY KEY (source_url_id, dest_url_id)
);

CREATE TABLE system_counters (
    counter_name TEXT PRIMARY KEY,
    value BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ
);


CREATE OR REPLACE FUNCTION url_content_search_vector_update() RETURNS trigger AS $$
BEGIN
    new.search_vector :=
        setweight(to_tsvector('english', coalesce(new.title, '')), 'A') ||
        setweight(to_tsvector('english', coalesce(new.description, '')), 'B') ||
        setweight(to_tsvector('english', coalesce(new.content, '')), 'D');
    return new;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER tsvectorupdate BEFORE INSERT OR UPDATE
ON url_content FOR EACH ROW EXECUTE PROCEDURE url_content_search_vector_update();


CREATE INDEX idx_urls_netloc ON urls (netloc);
CREATE INDEX idx_urls_pending_classification ON urls (id) WHERE status = 'pending_classification';
CREATE INDEX idx_urls_pending_crawl ON urls (id) WHERE status = 'pending_crawl';
CREATE INDEX idx_url_content_search_vector ON url_content USING GIN(search_vector);
INSERT INTO system_counters (counter_name) VALUES ('pending_urls_count');

