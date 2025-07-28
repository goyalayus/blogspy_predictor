--- FILE: src/woker/query.sql ---
-- woker/query.sql

-- name: LockJobsForUpdate :many
SELECT id, url FROM urls
WHERE status = $1
FOR UPDATE SKIP LOCKED
LIMIT $2;

-- name: UpdateJobStatusToInProgress :exec
UPDATE urls
SET status = $1, locked_at = NOW()
WHERE id = ANY(@job_ids::bigint[]);

-- name: UpdateStatus :exec
UPDATE urls
SET status = $1, error_message = $2, processed_at = NOW()
WHERE id = $3;

-- name: UpdateStatusAndRendering :exec
UPDATE urls
SET status = $1, rendering = $2, error_message = $3, processed_at = NOW()
WHERE id = $4;

-- name: UpdateURLAsCompleted :exec
UPDATE urls
SET status = $1, processed_at = NOW(), title = $2, description = $3, content = $4, rendering = $5
WHERE id = $6;

-- name: ResetStalledJobs :exec
UPDATE urls
SET
    status = CASE
        WHEN status = 'classifying'::crawl_status THEN 'pending_classification'::crawl_status
        WHEN status = 'crawling'::crawl_status THEN 'pending_crawl'::crawl_status
    END,
    locked_at = NULL
WHERE
    status IN ('classifying'::crawl_status, 'crawling'::crawl_status)
    AND locked_at < NOW() - sqlc.arg('timeout_interval')::interval;

-- name: GetNetlocCounts :many
-- MODIFIED: This now reads from the fast cache table instead of the huge urls table.
SELECT netloc, url_count FROM netloc_counts
WHERE netloc = ANY(@netlocs::text[]);

-- name: GetExistingURLs :many
SELECT url FROM urls
WHERE url = ANY(@urls::text[]);

-- name: GetDomainDecisions :many
SELECT DISTINCT ON (netloc) netloc, status
FROM urls
WHERE netloc = ANY(@netlocs::text[])
  AND status IN ('pending_crawl', 'crawling', 'completed', 'irrelevant');

-- QUERIES FOR THE REAPER AND THROTTLING MECHANISM

-- name: GetCounterValue :one
SELECT value FROM system_counters WHERE counter_name = $1;

-- name: UpdateCounterValue :exec
UPDATE system_counters SET value = $1, updated_at = NOW() WHERE counter_name = $2;

-- name: CountPendingURLs :one
SELECT count(*)::bigint FROM urls WHERE status IN ('pending_classification', 'pending_crawl');

-- NEW: Query for the Reaper to rebuild the netloc_counts cache table efficiently.
-- name: RefreshNetlocCounts :exec
INSERT INTO netloc_counts (netloc, url_count, updated_at)
SELECT netloc, COUNT(id)::int, NOW()
FROM urls
GROUP BY netloc
ON CONFLICT (netloc) DO UPDATE
SET url_count = EXCLUDED.url_count,
    updated_at = EXCLUDED.updated_at;
