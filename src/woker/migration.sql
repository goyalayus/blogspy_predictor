-- BlogSpy: Migration from Single 'urls' Table to 'urls' + 'url_content'
-- This script is designed to be run ONCE and is wrapped in a transaction.
-- If any part fails, the entire migration will be rolled back.
--
-- ⚠️  WARNING: BACK UP YOUR DATABASE BEFORE RUNNING THIS SCRIPT. ⚠️

BEGIN;

-- Lock the tables to prevent concurrent writes during migration.
LOCK TABLE urls IN SHARE ROW EXCLUSIVE MODE;

-- Step 1: Create the new table for storing "write-once" content.
-- It is UNLOGGED for maximum write performance, as the data is non-critical.
CREATE UNLOGGED TABLE IF NOT EXISTS url_content (
    url_id BIGINT PRIMARY KEY REFERENCES urls(id) ON DELETE CASCADE,
    title TEXT,
    description TEXT,
    content TEXT
);

-- Step 2: Migrate the content from existing 'completed' URLs into the new table.
-- We only care about the content of jobs that were successfully completed.
INSERT INTO url_content (url_id, title, description, content)
SELECT id, title, description, content
FROM urls
WHERE
    status = 'completed'
    AND title IS NOT NULL OR description IS NOT NULL OR content IS NOT NULL
ON CONFLICT (url_id) DO NOTHING; -- Safety: In case the script is re-run, do not fail.

-- Step 3: Drop the now-redundant large columns from the main 'urls' table.
-- This makes the 'urls' table much smaller and more efficient for job queueing.
ALTER TABLE urls DROP COLUMN IF EXISTS title;
ALTER TABLE urls DROP COLUMN IF EXISTS description;
ALTER TABLE urls DROP COLUMN IF EXISTS content;

-- Step 4: Safely create other new tables and indexes that are part of the new schema.
-- Using 'IF NOT EXISTS' makes these operations idempotent (safe to re-run).

-- Ensure ENUM types exist without erroring if they are already there.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'crawl_status') THEN
        CREATE TYPE crawl_status AS ENUM (
            'pending_classification', 'pending_crawl', 'classifying', 'crawling',
            'completed', 'failed', 'irrelevant'
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'rendering_type') THEN
        CREATE TYPE rendering_type AS ENUM ('SSR', 'CSR');
    END IF;
END$$;


-- Create new cache/counter tables
CREATE TABLE IF NOT EXISTS system_counters (
    counter_name TEXT PRIMARY KEY,
    value BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ
);

INSERT INTO system_counters (counter_name) VALUES ('pending_urls_count') ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS netloc_counts (
    netloc TEXT PRIMARY KEY,
    url_count INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL
);

-- Drop the old, less efficient index if it exists.
DROP INDEX IF EXISTS idx_urls_status;

-- Create the new, highly efficient partial indexes for the job queue.
CREATE INDEX IF NOT EXISTS idx_urls_pending_classification ON urls (id)
WHERE status = 'pending_classification';

CREATE INDEX IF NOT EXISTS idx_urls_pending_crawl ON urls (id)
WHERE status = 'pending_crawl';


-- If we reached here, all steps were successful.
COMMIT;

-- Step 5 (Post-Migration Maintenance): Reclaim disk space.
-- This must be run separately, outside the transaction, after commit.
-- It will lock the 'urls' table exclusively and can take time.
VACUUM (FULL, ANALYZE) urls;
