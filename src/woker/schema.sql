-- This is the complete and consolidated database schema for OrangeSearch.
-- It includes tables for user authentication, web crawling, content storage, and full-text search.

-- =================================================================
-- ENUM Types
-- =================================================================

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

-- =================================================================
-- Authentication Tables
-- =================================================================

CREATE TABLE "orange_users" (
    "id" serial PRIMARY KEY NOT NULL,
    "google_id" text NOT NULL,
    "email" varchar NOT NULL,
    "name" text NOT NULL,
    "picture" text NOT NULL,
    CONSTRAINT "orange_users_google_id_unique" UNIQUE("google_id"),
    CONSTRAINT "orange_users_email_unique" UNIQUE("email")
);

CREATE TABLE "orange_sessions" (
    "id" text PRIMARY KEY NOT NULL,
    "user_id" integer NOT NULL,
    "expires_at" timestamp with time zone NOT NULL
);

-- =================================================================
-- Crawler & Content Tables
-- =================================================================

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
    -- Column for storing pre-calculated PageRank score.
    pagerank_score REAL DEFAULT 1.0
    -- DELETED: title, description, content have been moved.
);

-- NEW: This is the "cold" table for storing "write-once" content.
-- It is UNLOGGED for maximum write performance, as the data is non-critical
-- and can be repopulated by re-crawling. It will be truncated on DB crash.
CREATE UNLOGGED TABLE url_content (
    url_id BIGINT PRIMARY KEY REFERENCES urls(id) ON DELETE CASCADE,
    title TEXT,
    description TEXT,
    content TEXT,
    -- Column for the pre-processed search vector.
    search_vector TSVECTOR
);

CREATE TABLE url_edges (
    source_url_id BIGINT NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
    dest_url_id BIGINT NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
    PRIMARY KEY (source_url_id, dest_url_id)
);

-- =================================================================
-- System & Counter Tables
-- =================================================================

CREATE TABLE system_counters (
    counter_name TEXT PRIMARY KEY,
    value BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ
);

CREATE TABLE netloc_counts (
    netloc TEXT PRIMARY KEY,
    url_count INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE classification_queue (
    id BIGSERIAL PRIMARY KEY,
    url_id BIGINT NOT NULL UNIQUE REFERENCES urls(id) ON DELETE CASCADE,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'new',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_at TIMESTAMPTZ
);

-- =================================================================
-- Foreign Keys
-- =================================================================

-- Link sessions to users. When a user is deleted, their sessions are also deleted.
ALTER TABLE "orange_sessions" ADD CONSTRAINT "orange_sessions_user_id_orange_users_id_fk"
    FOREIGN KEY ("user_id") REFERENCES "public"."orange_users"("id")
    ON DELETE CASCADE ON UPDATE NO ACTION;

-- Link content to a URL.
ALTER TABLE url_content ADD CONSTRAINT "url_content_url_id_fkey"
    FOREIGN KEY (url_id) REFERENCES urls(id)
    ON DELETE CASCADE;

-- Link edges to source and destination URLs.
ALTER TABLE url_edges ADD CONSTRAINT "url_edges_source_url_id_fkey"
    FOREIGN KEY (source_url_id) REFERENCES urls(id)
    ON DELETE CASCADE;

ALTER TABLE url_edges ADD CONSTRAINT "url_edges_dest_url_id_fkey"
    FOREIGN KEY (dest_url_id) REFERENCES urls(id)
    ON DELETE CASCADE;

-- =================================================================
-- Indexes for Performance
-- =================================================================

-- Authentication Indexes
CREATE INDEX "session_user_id_idx" ON "orange_sessions" USING btree ("user_id");
CREATE INDEX "google_id_idx" ON "orange_users" USING btree ("google_id");
CREATE INDEX "email_idx" ON "orange_users" USING btree ("email");

CREATE INDEX idx_urls_netloc ON urls (netloc);

DROP INDEX IF EXISTS idx_urls_status;

CREATE INDEX idx_urls_pending_classification ON urls (id)
WHERE status = 'pending_classification';

CREATE INDEX idx_urls_pending_crawl ON urls (id)
WHERE status = 'pending_crawl';

CREATE INDEX idx_classification_queue_status ON classification_queue (status);

-- Search Performance Indexes
CREATE INDEX idx_urls_pagerank_score ON urls (pagerank_score DESC);

-- The GIN index is critical for making full-text search fast.
CREATE INDEX idx_url_content_search_vector ON url_content USING GIN(search_vector);

-- =================================================================
-- Triggers for Automatic Search Vector Updates
-- =================================================================

-- This function automatically combines and weights content into the search_vector.
CREATE OR REPLACE FUNCTION update_search_vector()
RETURNS TRIGGER AS $$
BEGIN
    -- Combine title, description, and content into a single tsvector.
    -- Assign different weights: 'A' for title (highest), 'B' for description, 'D' for content (lowest).
    -- Use COALESCE to handle potential NULL values gracefully.
    NEW.search_vector :=
        setweight(to_tsvector('english', COALESCE(NEW.title, '')), 'A') ||
        setweight(to_tsvector('english', COALESCE(NEW.description, '')), 'B') ||
        setweight(to_tsvector('english', COALESCE(NEW.content, '')), 'D');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- This trigger fires the function before any insert or update on url_content.
CREATE TRIGGER tsvector_update_trigger
BEFORE INSERT OR UPDATE ON url_content
FOR EACH ROW EXECUTE FUNCTION update_search_vector();

-- =================================================================
-- Initial Data
-- =================================================================

INSERT INTO system_counters (counter_name) VALUES ('pending_urls_count');
