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

CREATE TABLE "search_history" (
    "id" BIGSERIAL PRIMARY KEY,
    "user_id" INTEGER REFERENCES orange_users(id) ON DELETE SET NULL,
    "ip_address" TEXT,
    "query" TEXT NOT NULL,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
    pagerank_score REAL DEFAULT 1.0
);

CREATE TABLE url_content (
    url_id BIGINT PRIMARY KEY REFERENCES urls(id) ON DELETE CASCADE,
    title TEXT,
    description TEXT,
    content TEXT,
    -- This column will store pre-computed tsvector for full-text search
    search_vector tsvector
);

CREATE TABLE url_edges (
    source_url_id BIGINT NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
    dest_url_id BIGINT NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
    PRIMARY KEY (source_url_id, dest_url_id)
);

-- System & Helper Tables
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


-- ========= Foreign Keys =========
ALTER TABLE "orange_sessions" ADD CONSTRAINT "orange_sessions_user_id_orange_users_id_fk" 
    FOREIGN KEY ("user_id") REFERENCES "public"."orange_users"("id") 
    ON DELETE CASCADE ON UPDATE NO ACTION;


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
-- User management indexes
CREATE INDEX "session_user_id_idx" ON "orange_sessions" USING btree ("user_id");
CREATE INDEX "google_id_idx" ON "orange_users" USING btree ("google_id");
CREATE INDEX "email_idx" ON "orange_users" USING btree ("email");
CREATE INDEX "idx_search_history_user_id" ON "search_history" USING btree ("user_id");
CREATE INDEX "idx_search_history_ip_address" ON "search_history" USING btree ("ip_address") WHERE "user_id" IS NULL;

-- Crawler indexes
CREATE INDEX idx_urls_netloc ON urls (netloc);
CREATE INDEX idx_urls_pending_classification ON urls (id) WHERE status = 'pending_classification';
CREATE INDEX idx_urls_pending_crawl ON urls (id) WHERE status = 'pending_crawl';

-- Full-text search index (crucial for performance)
CREATE INDEX idx_url_content_search_vector ON url_content USING GIN(search_vector);


-- ========= Initial Data =========
INSERT INTO system_counters (counter_name) VALUES ('pending_urls_count');

