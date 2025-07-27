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
SELECT netloc, count(id)::int as count FROM urls
WHERE netloc = ANY(@netlocs::text[])
GROUP BY netloc;

-- name: GetExistingURLs :many
SELECT url FROM urls
WHERE url = ANY(@urls::text[]);

-- name: GetDomainDecisions :many
SELECT DISTINCT ON (netloc) netloc, status
FROM urls
WHERE netloc = ANY(@netlocs::text[])
  AND status IN ('pending_crawl', 'crawling', 'completed', 'irrelevant');
