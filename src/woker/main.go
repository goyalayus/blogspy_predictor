// main
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

var (
	dbpool     *pgxpool.Pool
	httpClient *http.Client
	linkQueue  chan LinkBatch
	appConfig  Config
)

type Config struct {
	DatabaseURL         string
	MLApiURL            string
	BatchSize           int
	MaxWorkers          int
	SleepInterval       time.Duration
	JobTimeout          time.Duration
	FetchTimeout        time.Duration
	MaxUrlsPerNetloc    int
	BatchWriteInterval  time.Duration
	BatchWriteQueueSize int
	RestrictedTLDs      []string
	AllowedPathPrefixes []string
	IgnoreExtensions    []string
}
type CrawlStatus string

const (
	PendingClassification CrawlStatus = "pending_classification"
	PendingCrawl          CrawlStatus = "pending_crawl"
	Classifying           CrawlStatus = "classifying"
	Crawling              CrawlStatus = "crawling"
	Completed             CrawlStatus = "completed"
	Failed                CrawlStatus = "failed"
	Irrelevant            CrawlStatus = "irrelevant"
)

type RenderingType string

const (
	SSR RenderingType = "SSR"
	CSR RenderingType = "CSR"
)

type URLRecord struct {
	ID  int64  `db:"id"`
	URL string `db:"url"`
}
type FetchedContent struct {
	IsNonHTML, IsCSR                                       bool
	FinalURL, HTMLContent, TextContent, Title, Description string
	GoqueryDoc                                             *goquery.Document
}
type PredictionRequest struct {
	URL         string `json:"url"`
	HTMLContent string `json:"html_content"`
	TextContent string `json:"text_content"`
}
type PredictionResponse struct {
	IsPersonalBlog bool `json:"is_personal_blog"`
}
type LinkBatch struct {
	SourceURLID int64
	NewLinks    []NewLink
}
type NewLink struct {
	URL, Netloc string
	Status      CrawlStatus
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	slog.Info("--- Starting BlogSpy Go Worker ---")
	loadConfig()
	var err error
	dbpool, err = pgxpool.New(ctx, appConfig.DatabaseURL)
	if err != nil {
		slog.Error("Unable to create connection pool", "error", err)
		os.Exit(1)
	}
	defer dbpool.Close()
	slog.Info("Database connection pool established")
	httpClient = &http.Client{Timeout: appConfig.FetchTimeout}
	linkQueue = make(chan LinkBatch, appConfig.BatchWriteQueueSize)
	go databaseWriter(ctx)
	slog.Info("Database writer goroutine started")
	ticker := time.NewTicker(appConfig.SleepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			slog.Info("Shutdown signal received. Exiting...")
			return
		case <-ticker.C:
			slog.Debug("Worker cycle starting")
			resetStalledJobs(ctx)
			processJobType(ctx, "classification", PendingClassification, Classifying, processClassificationTask)
			processJobType(ctx, "crawling", PendingCrawl, Crawling, processCrawlTask)
		}
	}
}

type taskFunc func(context.Context, URLRecord) error

func processJobType(ctx context.Context, jobType string, fromStatus, toStatus CrawlStatus, task taskFunc) {
	tx, err := dbpool.Begin(ctx)
	if err != nil {
		slog.Error("Failed to begin transaction", "error", err)
		return
	}
	defer tx.Rollback(ctx)
	query := `SELECT id, url FROM urls WHERE status = $1 FOR UPDATE SKIP LOCKED LIMIT $2`
	rows, err := tx.Query(ctx, query, fromStatus, appConfig.BatchSize)
	if err != nil {
		slog.Error("Failed to query for jobs", "type", jobType, "error", err)
		return
	}
	jobs, err := pgx.CollectRows(rows, pgx.RowToStructByName[URLRecord])
	if err != nil {
		slog.Error("Failed to scan job rows", "error", err)
		return
	}
	if len(jobs) == 0 {
		return
	}
	jobIDs := make([]int64, len(jobs))
	for i, job := range jobs {
		jobIDs[i] = job.ID
	}
	updateQuery := `UPDATE urls SET status = $1, locked_at = NOW() WHERE id = ANY($2)`
	if _, err := tx.Exec(ctx, updateQuery, toStatus, jobIDs); err != nil {
		slog.Error("Failed to lock jobs", "type", jobType, "error", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		slog.Error("Failed to commit job-locking transaction", "error", err)
		return
	}
	slog.Info("Locked and dispatched jobs", "type", jobType, "count", len(jobs))
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(appConfig.MaxWorkers)
	for _, job := range jobs {
		currentJob := job
		g.Go(func() error {
			if err := task(gCtx, currentJob); err != nil {
				slog.Error("Task failed", "job_id", currentJob.ID, "url", currentJob.URL, "error", err)
			}
			return nil
		})
	}
	_ = g.Wait()
	slog.Info("Finished processing batch", "type", jobType, "count", len(jobs))
}

func withTransaction(ctx context.Context, fn func(tx pgx.Tx) error) (err error) {
	tx, err := dbpool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(ctx)
			panic(p)
		} else if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				slog.Error("Error during transaction rollback", "original_error", err, "rollback_error", rbErr)
			}
		} else {
			err = tx.Commit(ctx)
		}
	}()
	err = fn(tx)
	return err
}

func processClassificationTask(ctx context.Context, job URLRecord) error {
	return withTransaction(ctx, func(tx pgx.Tx) error {
		content, err := fetchAndParseContent(ctx, job.URL)
		if err != nil {
			return updateStatus(ctx, tx, job.ID, Failed, err.Error())
		}
		if content.IsNonHTML {
			return updateStatus(ctx, tx, job.ID, Irrelevant, "Content-Type was not HTML")
		}
		if content.IsCSR {
			return updateStatusAndRendering(ctx, tx, job.ID, Irrelevant, CSR, "Detected Client-Side Rendering")
		}

		reqBody := PredictionRequest{URL: content.FinalURL, HTMLContent: content.HTMLContent, TextContent: content.TextContent}
		jsonBody, _ := json.Marshal(reqBody)
		httpReq, _ := http.NewRequestWithContext(ctx, "POST", appConfig.MLApiURL+"/predict", bytes.NewBuffer(jsonBody))
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := httpClient.Do(httpReq)
		if err != nil {
			return updateStatus(ctx, tx, job.ID, Failed, "Prediction API call failed: "+err.Error())
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			errMsg := fmt.Sprintf("Prediction API returned non-200 status: %d - %s", resp.StatusCode, string(bodyBytes))
			return updateStatus(ctx, tx, job.ID, Failed, errMsg)
		}

		var predResp PredictionResponse
		if err := json.NewDecoder(resp.Body).Decode(&predResp); err != nil {
			return updateStatus(ctx, tx, job.ID, Failed, "Failed to decode prediction response: "+err.Error())
		}

		if predResp.IsPersonalBlog {
			if err := performCrawlLogic(ctx, tx, job, content); err != nil {
				return err
			}
			return updateURLAsCompleted(ctx, tx, job.ID, content, Completed)
		}
		return updateURLAsCompleted(ctx, tx, job.ID, content, Irrelevant)
	})
}

func processCrawlTask(ctx context.Context, job URLRecord) error {
	return withTransaction(ctx, func(tx pgx.Tx) error {
		content, err := fetchAndParseContent(ctx, job.URL)
		if err != nil {
			return updateStatus(ctx, tx, job.ID, Failed, err.Error())
		}
		if content.IsNonHTML {
			return updateStatus(ctx, tx, job.ID, Completed, "Content-Type was not HTML")
		}
		if content.IsCSR {
			return updateStatusAndRendering(ctx, tx, job.ID, Completed, CSR, "Detected CSR during crawl")
		}
		if err := performCrawlLogic(ctx, tx, job, content); err != nil {
			return err
		}
		return updateURLAsCompleted(ctx, tx, job.ID, content, Completed)
	})
}

func performCrawlLogic(ctx context.Context, tx pgx.Tx, job URLRecord, content *FetchedContent) error {
	newLinks := extractLinks(content.GoqueryDoc, content.FinalURL)
	if len(newLinks) == 0 {
		return nil
	}
	linksByNetloc := make(map[string][]string)
	var allNetlocsFound []string
	netlocSet := make(map[string]struct{})
	for _, link := range newLinks {
		parsed, err := url.Parse(link)
		if err != nil || parsed.Host == "" {
			continue
		}
		netloc := parsed.Host
		linksByNetloc[netloc] = append(linksByNetloc[netloc], link)
		if _, exists := netlocSet[netloc]; !exists {
			netlocSet[netloc] = struct{}{}
			allNetlocsFound = append(allNetlocsFound, netloc)
		}
	}
	if len(allNetlocsFound) == 0 {
		return nil
	}
	netlocCounts := make(map[string]int)
	countQuery := `SELECT netloc, count(id) FROM urls WHERE netloc = ANY($1) GROUP BY netloc`
	rows, err := tx.Query(ctx, countQuery, allNetlocsFound)
	if err != nil {
		return fmt.Errorf("crawl-logic: failed to get netloc counts: %w", err)
	}
	netlocPtr := new(string)
	countPtr := new(int64)
	_, err = pgx.ForEachRow(rows, []any{netlocPtr, countPtr}, func() error {
		netlocCounts[*netlocPtr] = int(*countPtr)
		return nil
	})
	if err != nil {
		return fmt.Errorf("crawl-logic: failed scanning netloc counts: %w", err)
	}
	existingURLs := make(map[string]struct{})
	existQuery := `SELECT url FROM urls WHERE url = ANY($1)`
	rows, err = tx.Query(ctx, existQuery, newLinks)
	if err != nil {
		return fmt.Errorf("crawl-logic: failed to check existing urls: %w", err)
	}
	urlPtr := new(string)
	_, err = pgx.ForEachRow(rows, []any{urlPtr}, func() error {
		existingURLs[*urlPtr] = struct{}{}
		return nil
	})
	if err != nil {
		return fmt.Errorf("crawl-logic: failed scanning existing urls: %w", err)
	}
	domainDecisions := make(map[string]CrawlStatus)
	decisionQuery := `SELECT DISTINCT ON (netloc) netloc, status FROM urls WHERE netloc = ANY($1) AND status IN ($2, $3, $4, $5)`
	rows, err = tx.Query(ctx, decisionQuery, allNetlocsFound, PendingCrawl, Crawling, Completed, Irrelevant)
	if err != nil {
		return fmt.Errorf("crawl-logic: failed to get domain decisions: %w", err)
	}
	statusPtr := new(CrawlStatus)
	_, err = pgx.ForEachRow(rows, []any{netlocPtr, statusPtr}, func() error {
		domainDecisions[*netlocPtr] = *statusPtr
		return nil
	})
	if err != nil {
		return fmt.Errorf("crawl-logic: failed scanning domain decisions: %w", err)
	}
	sourceNetloc, err := url.Parse(job.URL)
	if err != nil {
		return fmt.Errorf("crawl-logic: could not parse source job URL %q: %w", job.URL, err)
	}
	var linksToQueue []NewLink
	for netloc, links := range linksByNetloc {
		if netlocCounts[netloc] >= appConfig.MaxUrlsPerNetloc {
			continue
		}
		for _, link := range links {
			if _, exists := existingURLs[link]; exists {
				continue
			}
			isRestricted := false
			for _, tld := range appConfig.RestrictedTLDs {
				if strings.HasSuffix(netloc, tld) {
					isRestricted = true
					break
				}
			}
			if isRestricted {
				path := getPath(link)
				isAllowed := false
				for _, prefix := range appConfig.AllowedPathPrefixes {
					if strings.HasPrefix(path, prefix) || path == "/" || path == "" {
						isAllowed = true
						break
					}
				}
				if !isAllowed {
					continue
				}
			}
			linkStatus := PendingClassification
			if netloc == sourceNetloc.Host {
				linkStatus = PendingCrawl
			} else if decision, ok := domainDecisions[netloc]; ok {
				if decision == Irrelevant {
					linkStatus = Irrelevant
				} else {
					linkStatus = PendingCrawl
				}
			}
			linksToQueue = append(linksToQueue, NewLink{URL: link, Netloc: netloc, Status: linkStatus})
			netlocCounts[netloc]++
			if netlocCounts[netloc] >= appConfig.MaxUrlsPerNetloc {
				break
			}
		}
	}
	if len(linksToQueue) > 0 {
		select {
		case linkQueue <- LinkBatch{SourceURLID: job.ID, NewLinks: linksToQueue}:
		default:
			slog.Warn("Link queue is full. Dropping links.", "count", len(linksToQueue))
		}
	}
	return nil
}
func databaseWriter(ctx context.Context) {
	ticker := time.NewTicker(appConfig.BatchWriteInterval)
	defer ticker.Stop()
	var batches []LinkBatch
	for {
		select {
		case <-ctx.Done():
			if len(batches) > 0 {
				slog.Info("DB Writer: Final write on shutdown...")
				processLinkBatches(context.Background(), batches)
			}
			slog.Info("DB Writer: Shutdown.")
			return
		case batch := <-linkQueue:
			batches = append(batches, batch)
		case <-ticker.C:
			if len(batches) > 0 {
				processLinkBatches(ctx, batches)
				batches = nil
			}
		}
	}
}
func processLinkBatches(ctx context.Context, batches []LinkBatch) {
	allNewLinks := make(map[string]NewLink)
	for _, batch := range batches {
		for _, link := range batch.NewLinks {
			allNewLinks[link.URL] = link
		}
	}
	if len(allNewLinks) == 0 {
		return
	}
	err := withTransaction(ctx, func(tx pgx.Tx) error {
		var urlRows [][]any
		for _, link := range allNewLinks {
			urlRows = append(urlRows, []any{link.URL, link.Netloc, link.Status})
		}
		if _, err := tx.Exec(ctx, `CREATE TEMP TABLE new_urls (url TEXT, netloc TEXT, status crawl_status) ON COMMIT DROP`); err != nil {
			return fmt.Errorf("failed to create temp table: %w", err)
		}
		_, err := tx.CopyFrom(ctx, pgx.Identifier{"new_urls"}, []string{"url", "netloc", "status"}, pgx.CopyFromRows(urlRows))
		if err != nil {
			return fmt.Errorf("failed to copy to temp table: %w", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO urls (url, netloc, status) SELECT url, netloc, status FROM new_urls ON CONFLICT (url) DO NOTHING`); err != nil {
			return fmt.Errorf("failed to insert from temp table: %w", err)
		}
		urlStrings := make([]string, 0, len(allNewLinks))
		for urlStr := range allNewLinks {
			urlStrings = append(urlStrings, urlStr)
		}
		rows, err := tx.Query(ctx, `SELECT id, url FROM urls WHERE url = ANY($1)`, urlStrings)
		if err != nil {
			return fmt.Errorf("failed to query for new URL IDs: %w", err)
		}
		urlToIDMap := make(map[string]int64)
		idPtr := new(int64)
		urlPtr := new(string)
		if _, err := pgx.ForEachRow(rows, []any{idPtr, urlPtr}, func() error {
			urlToIDMap[*urlPtr] = *idPtr
			return nil
		}); err != nil {
			return fmt.Errorf("failed to iterate new URL rows: %w", err)
		}
		var edgeRows [][]any
		for _, batch := range batches {
			for _, link := range batch.NewLinks {
				if destID, ok := urlToIDMap[link.URL]; ok {
					edgeRows = append(edgeRows, []any{batch.SourceURLID, destID})
				}
			}
		}
		if len(edgeRows) > 0 {
			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"url_edges"}, []string{"source_url_id", "dest_url_id"}, pgx.CopyFromRows(edgeRows)); err != nil {
				if !strings.Contains(err.Error(), "23505") {
					return fmt.Errorf("failed to bulk insert edges: %w", err)
				}
			}
		}
		return nil
	})
	if err != nil {
		slog.Error("DB Writer: Transaction failed", "error", err)
	} else {
		slog.Info("DB Writer: Successfully committed batch", "potential_new_urls", len(allNewLinks))
	}
}
func fetchAndParseContent(ctx context.Context, rawURL string) (*FetchedContent, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("bad status code: %d", resp.StatusCode)
	}
	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(strings.ToLower(contentType), "html") {
		return &FetchedContent{IsNonHTML: true, FinalURL: resp.Request.URL.String()}, nil
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	htmlContent := string(bodyBytes)
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlContent))
	if err != nil {
		return nil, err
	}
	content := &FetchedContent{FinalURL: resp.Request.URL.String(), HTMLContent: htmlContent, GoqueryDoc: doc}
	if doc.Find("#root, #app, [data-reactroot]").Length() > 0 && len(strings.TrimSpace(doc.Find("body").Text())) < 250 {
		content.IsCSR = true
	}
	if doc.Find("template[data-dgst='BAILOUT_TO_CLIENT_SIDE_RENDERING']").Length() > 0 {
		content.IsCSR = true
	}
	if content.IsCSR {
		return content, nil
	}
	content.Title = strings.TrimSpace(doc.Find("title").First().Text())
	if val, exists := doc.Find("meta[name='description']").Attr("content"); exists {
		content.Description = strings.TrimSpace(val)
	}
	doc.Find("script, style, noscript").Remove()
	re := strings.NewReplacer("\n", " ", "\t", " ", "\r", " ")
	content.TextContent = strings.Join(strings.Fields(re.Replace(doc.Text())), " ")
	return content, nil
}
func extractLinks(doc *goquery.Document, baseURL string) []string {
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil
	}
	linkSet := make(map[string]struct{})
	doc.Find("a[href]").Each(func(i int, s *goquery.Selection) {
		href, _ := s.Attr("href")
		href = strings.TrimSpace(href)
		if href == "" || strings.HasPrefix(href, "#") || strings.HasPrefix(href, "mailto:") || strings.HasPrefix(href, "javascript:") {
			return
		}
		lowerHref := strings.ToLower(href)
		for _, ext := range appConfig.IgnoreExtensions {
			if strings.HasSuffix(lowerHref, ext) {
				return
			}
		}
		resolvedURL, err := base.Parse(href)
		if err == nil && (resolvedURL.Scheme == "http" || resolvedURL.Scheme == "https") {
			resolvedURL.Fragment = ""
			linkSet[resolvedURL.String()] = struct{}{}
		}
	})
	links := make([]string, 0, len(linkSet))
	for link := range linkSet {
		links = append(links, link)
	}
	return links
}
func getPath(rawURL string) string {
	if parsed, err := url.Parse(rawURL); err == nil {
		return parsed.Path
	}
	return ""
}
func updateStatus(ctx context.Context, tx pgx.Tx, id int64, status CrawlStatus, errMsg string) error {
	query := `UPDATE urls SET status = $1, error_message = $2, processed_at = NOW() WHERE id = $3`
	_, err := tx.Exec(ctx, query, status, errMsg, id)
	if err != nil {
		slog.Error("Failed to update status", "id", id, "error", err)
	}
	return err
}
func updateStatusAndRendering(ctx context.Context, tx pgx.Tx, id int64, status CrawlStatus, rendering RenderingType, errMsg string) error {
	query := `UPDATE urls SET status = $1, rendering = $2, error_message = $3, processed_at = NOW() WHERE id = $4`
	_, err := tx.Exec(ctx, query, status, rendering, errMsg, id)
	if err != nil {
		slog.Error("Failed to update status and rendering", "id", id, "error", err)
	}
	return err
}
func updateURLAsCompleted(ctx context.Context, tx pgx.Tx, id int64, content *FetchedContent, status CrawlStatus) error {
	query := `UPDATE urls SET status = $1, processed_at = NOW(), title = $2, description = $3, content = $4, rendering = $5 WHERE id = $6`
	_, err := tx.Exec(ctx, query, status, content.Title, content.Description, content.TextContent, SSR, id)
	if err != nil {
		slog.Error("Failed to update URL as completed", "id", id, "error", err)
	}
	return err
}
func resetStalledJobs(ctx context.Context) {
	query := `UPDATE urls SET status = CASE WHEN status = 'classifying'::crawl_status THEN 'pending_classification'::crawl_status WHEN status = 'crawling'::crawl_status THEN 'pending_crawl'::crawl_status END, locked_at = NULL WHERE status IN ('classifying'::crawl_status, 'crawling'::crawl_status) AND locked_at < NOW() - $1::interval`
	interval := fmt.Sprintf("%f minutes", appConfig.JobTimeout.Minutes())
	res, err := dbpool.Exec(ctx, query, interval)
	if err != nil {
		slog.Error("Reaper: Failed to reset stalled jobs", "error", err)
	} else if res.RowsAffected() > 0 {
		slog.Warn("Reaper: Reset stalled jobs", "count", res.RowsAffected())
	}
}
func getEnv(key, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}
func loadConfig() {
	var missingVars []string
	appConfig.DatabaseURL = getEnv("DATABASE_URL", "")
	appConfig.MLApiURL = getEnv("ML_API_URL", "")
	if appConfig.DatabaseURL == "" {
		missingVars = append(missingVars, "DATABASE_URL")
	}
	if appConfig.MLApiURL == "" {
		missingVars = append(missingVars, "ML_API_URL")
	}
	if len(missingVars) > 0 {
		slog.Error("Missing required environment variables", "missing", strings.Join(missingVars, ", "))
		os.Exit(1)
	}
	var err error
	appConfig.BatchSize, err = strconv.Atoi(getEnv("BATCH_SIZE", "500"))
	if err != nil {
		slog.Warn("Invalid BATCH_SIZE", "value", getEnv("BATCH_SIZE", "500"), "error", err)
		appConfig.BatchSize = 500
	}
	appConfig.MaxWorkers, err = strconv.Atoi(getEnv("MAX_WORKERS", "50"))
	if err != nil {
		slog.Warn("Invalid MAX_WORKERS", "value", getEnv("MAX_WORKERS", "50"), "error", err)
		appConfig.MaxWorkers = 50
	}
	appConfig.SleepInterval, err = time.ParseDuration(getEnv("SLEEP_INTERVAL", "5s"))
	if err != nil {
		slog.Warn("Invalid SLEEP_INTERVAL", "value", getEnv("SLEEP_INTERVAL", "5s"), "error", err)
		appConfig.SleepInterval = 5 * time.Second
	}
	appConfig.JobTimeout, err = time.ParseDuration(getEnv("JOB_TIMEOUT", "15m"))
	if err != nil {
		slog.Warn("Invalid JOB_TIMEOUT", "value", getEnv("JOB_TIMEOUT", "15m"), "error", err)
		appConfig.JobTimeout = 15 * time.Minute
	}
	appConfig.FetchTimeout, err = time.ParseDuration(getEnv("FETCH_TIMEOUT", "6s"))
	if err != nil {
		slog.Warn("Invalid FETCH_TIMEOUT", "value", getEnv("FETCH_TIMEOUT", "6s"), "error", err)
		appConfig.FetchTimeout = 6 * time.Second
	}
	appConfig.MaxUrlsPerNetloc, err = strconv.Atoi(getEnv("MAX_URLS_PER_NETLOC", "130"))
	if err != nil {
		slog.Warn("Invalid MAX_URLS_PER_NETLOC", "value", getEnv("MAX_URLS_PER_NETLOC", "130"), "error", err)
		appConfig.MaxUrlsPerNetloc = 130
	}
	appConfig.BatchWriteInterval, err = time.ParseDuration(getEnv("BATCH_WRITE_INTERVAL", "10s"))
	if err != nil {
		slog.Warn("Invalid BATCH_WRITE_INTERVAL", "value", getEnv("BATCH_WRITE_INTERVAL", "10s"), "error", err)
		appConfig.BatchWriteInterval = 10 * time.Second
	}
	appConfig.BatchWriteQueueSize, err = strconv.Atoi(getEnv("BATCH_WRITE_QUEUE_SIZE", "1000"))
	if err != nil {
		slog.Warn("Invalid BATCH_WRITE_QUEUE_SIZE", "value", getEnv("BATCH_WRITE_QUEUE_SIZE", "1000"), "error", err)
		appConfig.BatchWriteQueueSize = 1000
	}
	appConfig.RestrictedTLDs = strings.Split(getEnv("RESTRICTED_TLDS", ".org,.edu"), ",")
	appConfig.AllowedPathPrefixes = strings.Split(getEnv("ALLOWED_PATH_PREFIXES", "/blog"), ",")
	appConfig.IgnoreExtensions = strings.Split(getEnv("IGNORE_EXTENSIONS", ".pdf,.jpg,.jpeg,.png,.gif,.zip,.rar,.exe,.mp3,.mp4,.avi,.mov,.dmg,.iso,.css,.js,.xml,.json,.gz,.tar,.tgz"), ",")
}
