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

-- DELETED: The 'GetNetlocCounts' query was here.

-- name: GetExistingURLs :many
SELECT url FROM urls
WHERE url = ANY(@urls::text[]);

-- QUERIES FOR THE REAPER AND THROTTLING MECHANISM

-- name: GetCounterValue :one
SELECT value FROM system_counters WHERE counter_name = $1;

-- name: UpdateCounterValue :exec
UPDATE system_counters SET value = $1, updated_at = NOW() WHERE counter_name = $2;

-- name: CountPendingURLs :one
SELECT count(*)::bigint FROM urls WHERE status IN ('pending_classification', 'pending_crawl');

-- DELETED: The 'RefreshNetlocCounts' query was here.

-- MODIFIED: Rewritten to use a more robust NOT EXISTS pattern.
-- This finds 'completed' jobs whose content was lost due to a crash.
-- name: ResetOrphanedCompletedJobs :execrows
UPDATE urls u
SET
    status = 'pending_crawl'::crawl_status,
    processed_at = NULL,
    error_message = NULL,
    locked_at = NULL
WHERE
    u.status = 'completed'
    AND NOT EXISTS (
        SELECT 1
        FROM url_content uc
        WHERE uc.url_id = u.id
    );
