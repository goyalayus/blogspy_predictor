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

var AppConfig Config

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
	ID  int64
	URL string
}

type FetchedContent struct {
	IsNonHTML   bool
	IsCSR       bool
	FinalURL    string
	HTMLContent string
	TextContent string
	Title       string
	Description string
	GoqueryDoc  *goquery.Document
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
	URL    string
	Netloc string
	Status CrawlStatus
}

var (
	dbpool     *pgxpool.Pool
	httpClient *http.Client
	linkQueue  chan LinkBatch
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("--- Starting BlogSpy Go Worker ---")
	loadConfig()

	var err error
	dbpool, err = pgxpool.New(ctx, AppConfig.DatabaseURL)
	if err != nil {
		slog.Error("Unable to create connection pool", "error", err)
		os.Exit(1)
	}
	defer dbpool.Close()
	slog.Info("Database connection pool established.")

	httpClient = &http.Client{Timeout: AppConfig.FetchTimeout}

	linkQueue = make(chan LinkBatch, AppConfig.BatchWriteQueueSize)
	go databaseWriter(ctx)
	slog.Info("Database writer goroutine started.")

	ticker := time.NewTicker(AppConfig.SleepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Shutdown signal received. Exiting...")
			return
		case <-ticker.C:
			slog.Debug("Worker cycle starting.")
			resetStalledJobs(ctx)
			processJobType(ctx, "classification", PendingClassification, Classifying)
			processJobType(ctx, "crawling", PendingCrawl, Crawling)
		}
	}
}

func processJobType(ctx context.Context, jobType string, fromStatus, toStatus CrawlStatus) {
	tx, err := dbpool.Begin(ctx)
	if err != nil {
		slog.Error("Failed to begin transaction", "error", err)
		return
	}
	defer tx.Rollback(ctx)

	query := `SELECT id, url FROM urls WHERE status = $1 FOR UPDATE SKIP LOCKED LIMIT $2`
	rows, err := tx.Query(ctx, query, fromStatus, AppConfig.BatchSize)
	if err != nil {
		slog.Error("Failed to query for jobs", "type", jobType, "error", err)
		return
	}

	var jobs []URLRecord
	jobs, err = pgx.CollectRows(rows, pgx.RowToStructByPos[URLRecord])
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
	g.SetLimit(AppConfig.MaxWorkers)

	for _, job := range jobs {
		g.Go(func() error {
			if jobType == "classification" {
				return processClassificationTask(gCtx, job)
			}
			return processCrawlTask(gCtx, job)
		})
	}

	if err := g.Wait(); err != nil {
		slog.Error("Error occurred during batch processing", "type", jobType, "error", err)
	}
	slog.Info("Finished processing batch", "type", jobType, "count", len(jobs))
}

func processClassificationTask(ctx context.Context, job URLRecord) (err error) {
	tx, err := dbpool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback(ctx)
			panic(p)
		} else if err != nil {
			tx.Rollback(ctx)
		} else {
			err = tx.Commit(ctx)
		}
	}()

	content, err := fetchAndParseContent(ctx, job.URL)
	if err != nil {
		slog.Error("Failed to fetch/parse content", "url", job.URL, "error", err)
		updateStatus(ctx, tx, job.ID, Failed, err.Error())
		return nil
	}
	if content.IsNonHTML {
		updateStatus(ctx, tx, job.ID, Irrelevant, "Content-Type was not HTML")
		return nil
	}
	if content.IsCSR {
		updateStatusAndRendering(ctx, tx, job.ID, Irrelevant, CSR, "Detected Client-Side Rendering")
		return nil
	}

	reqBody := PredictionRequest{URL: content.FinalURL, HTMLContent: content.HTMLContent, TextContent: content.TextContent}
	jsonBody, _ := json.Marshal(reqBody)
	httpReq, _ := http.NewRequestWithContext(ctx, "POST", AppConfig.MLApiURL+"/predict", bytes.NewBuffer(jsonBody))
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		slog.Error("Failed to call prediction API", "url", job.URL, "error", err)
		updateStatus(ctx, tx, job.ID, Failed, "Prediction API call failed: "+err.Error())
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		errMsg := fmt.Sprintf("Prediction API returned non-200 status: %d - %s", resp.StatusCode, string(bodyBytes))
		slog.Error(errMsg, "url", job.URL)
		updateStatus(ctx, tx, job.ID, Failed, errMsg)
		return nil
	}

	var predResp PredictionResponse
	if err := json.NewDecoder(resp.Body).Decode(&predResp); err != nil {
		slog.Error("Failed to decode prediction response", "url", job.URL, "error", err)
		updateStatus(ctx, tx, job.ID, Failed, "Failed to decode prediction response: "+err.Error())
		return nil
	}

	if predResp.IsPersonalBlog {
		performCrawlLogic(ctx, tx, job, content)
		updateURLAsCompleted(ctx, tx, job.ID, content, Completed)
	} else {
		updateURLAsCompleted(ctx, tx, job.ID, content, Irrelevant)
	}
	return nil
}

func processCrawlTask(ctx context.Context, job URLRecord) (err error) {
	tx, err := dbpool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback(ctx)
			panic(p)
		} else if err != nil {
			tx.Rollback(ctx)
		} else {
			err = tx.Commit(ctx)
		}
	}()

	content, err := fetchAndParseContent(ctx, job.URL)
	if err != nil {
		updateStatus(ctx, tx, job.ID, Failed, err.Error())
		return nil
	}
	if content.IsNonHTML {
		updateStatus(ctx, tx, job.ID, Completed, "Content-Type was not HTML")
		return nil
	}
	if content.IsCSR {
		updateStatusAndRendering(ctx, tx, job.ID, Completed, CSR, "Detected CSR during crawl")
		return nil
	}

	performCrawlLogic(ctx, tx, job, content)
	updateURLAsCompleted(ctx, tx, job.ID, content, Completed)
	return nil
}

func performCrawlLogic(ctx context.Context, tx pgx.Tx, job URLRecord, content *FetchedContent) {
	newLinks := extractLinks(content.GoqueryDoc, content.FinalURL)
	if len(newLinks) == 0 {
		return
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

	netlocCounts := make(map[string]int)
	countQuery := `SELECT netloc, count(id) FROM urls WHERE netloc = ANY($1) GROUP BY netloc`
	rows, _ := tx.Query(ctx, countQuery, allNetlocsFound)
	for rows.Next() {
		var netloc string
		var count int
		if rows.Scan(&netloc, &count) == nil {
			netlocCounts[netloc] = count
		}
	}
	rows.Close()

	existingURLs := make(map[string]struct{})
	existQuery := `SELECT url FROM urls WHERE url = ANY($1)`
	rows, _ = tx.Query(ctx, existQuery, newLinks)
	for rows.Next() {
		var u string
		if rows.Scan(&u) == nil {
			existingURLs[u] = struct{}{}
		}
	}
	rows.Close()

	domainDecisions := make(map[string]CrawlStatus)
	decisionQuery := `SELECT DISTINCT ON (netloc) netloc, status FROM urls WHERE netloc = ANY($1) AND status IN ($2, $3, $4, $5)`
	rows, _ = tx.Query(ctx, decisionQuery, allNetlocsFound, PendingCrawl, Crawling, Completed, Irrelevant)
	for rows.Next() {
		var netloc string
		var status CrawlStatus
		if rows.Scan(&netloc, &status) == nil {
			domainDecisions[netloc] = status
		}
	}
	rows.Close()

	var linksToQueue []NewLink
	sourceNetloc := getNetloc(job.URL)

	for netloc, links := range linksByNetloc {
		if netlocCounts[netloc] >= AppConfig.MaxUrlsPerNetloc {
			continue
		}
		for _, link := range links {
			isRestricted := false
			for _, tld := range AppConfig.RestrictedTLDs {
				if strings.HasSuffix(netloc, tld) {
					isRestricted = true
					break
				}
			}
			if isRestricted {
				path := getPath(link)
				isAllowed := false
				for _, prefix := range AppConfig.AllowedPathPrefixes {
					if strings.HasPrefix(path, prefix) || path == "/" || path == "" {
						isAllowed = true
						break
					}
				}
				if !isAllowed {
					continue
				}
			}
			if _, exists := existingURLs[link]; exists {
				continue
			}

			linkStatus := PendingClassification
			if netloc == sourceNetloc {
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
			if netlocCounts[netloc] >= AppConfig.MaxUrlsPerNetloc {
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
}

func databaseWriter(ctx context.Context) {
	ticker := time.NewTicker(AppConfig.BatchWriteInterval)
	defer ticker.Stop()
	var batches []LinkBatch
	for {
		select {
		case <-ctx.Done():
			return
		case batch := <-linkQueue:
			batches = append(batches, batch)
		case <-ticker.C:
			if len(batches) == 0 {
				continue
			}
		drainLoop:
			for {
				select {
				case batch := <-linkQueue:
					batches = append(batches, batch)
				default:
					break drainLoop
				}
			}
			processLinkBatches(ctx, batches)
			batches = nil
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

	tx, err := dbpool.Begin(ctx)
	if err != nil {
		slog.Error("DB Writer: Failed to begin transaction", "error", err)
		return
	}
	defer tx.Rollback(ctx)

	var urlRows [][]any
	for _, link := range allNewLinks {
		urlRows = append(urlRows, []any{link.URL, link.Netloc, link.Status})
	}
	_, err = tx.CopyFrom(ctx, pgx.Identifier{"urls"}, []string{"url", "netloc", "status"}, pgx.CopyFromRows(urlRows))
	if err != nil && !strings.Contains(err.Error(), "23505") {
		slog.Error("DB Writer: Failed to bulk insert urls", "error", err)
		return
	}

	urlStrings := make([]string, 0, len(allNewLinks))
	for urlStr := range allNewLinks {
		urlStrings = append(urlStrings, urlStr)
	}
	rows, err := tx.Query(ctx, `SELECT id, url FROM urls WHERE url = ANY($1)`, urlStrings)
	if err != nil {
		slog.Error("DB Writer: Failed to query for new URL IDs", "error", err)
		return
	}
	urlToIDMap := make(map[string]int64)
	for rows.Next() {
		var id int64
		var urlStr string
		if err := rows.Scan(&id, &urlStr); err == nil {
			urlToIDMap[urlStr] = id
		}
	}
	rows.Close()

	var edgeRows [][]any
	for _, batch := range batches {
		for _, link := range batch.NewLinks {
			if destID, ok := urlToIDMap[link.URL]; ok {
				edgeRows = append(edgeRows, []any{
					batch.SourceURLID,
					destID,
				})
			}
		}
	}
	if len(edgeRows) > 0 {
		_, err = tx.CopyFrom(ctx, pgx.Identifier{"url_edges"}, []string{
			"source_url_id",
			"dest_url_id",
		}, pgx.CopyFromRows(edgeRows))
		if err != nil && !strings.Contains(err.Error(), "23505") { // Ignore unique constraint errors
			slog.Error("DB Writer: Failed to bulk insert edges", "error", err)
			return
		}
	}
	if err := tx.Commit(ctx); err != nil {
		slog.Error("DB Writer: Failed to commit transaction", "error", err)
	} else {
		slog.Info("DB Writer: Successfully committed batch", "new_urls", len(urlRows), "new_edges", len(edgeRows))
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
	if doc.Find("#root, #app").Length() > 0 && len(strings.TrimSpace(doc.Find("body").Text())) < 250 {
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
	doc.Find("script, style").Remove()
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
		for _, ext := range AppConfig.IgnoreExtensions {
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

func getNetloc(rawURL string) string {
	if parsed, err := url.Parse(rawURL); err == nil {
		return parsed.Host
	}
	return ""
}
func getPath(rawURL string) string {
	if parsed, err := url.Parse(rawURL); err == nil {
		return parsed.Path
	}
	return ""
}
func updateStatus(ctx context.Context, tx pgx.Tx, id int64, status CrawlStatus, errMsg string) {
	query := `UPDATE urls SET status = $1, error_message = $2, processed_at = NOW() WHERE id = $3`
	_, err := tx.Exec(ctx, query, status, errMsg, id)
	if err != nil {
		slog.Error("Failed to update status", "id", id, "error", err)
	}
}
func updateStatusAndRendering(ctx context.Context, tx pgx.Tx, id int64, status CrawlStatus, rendering RenderingType, errMsg string) {
	query := `UPDATE urls SET status = $1, rendering = $2, error_message = $3, processed_at = NOW() WHERE id = $4`
	_, err := tx.Exec(ctx, query, status, rendering, errMsg, id)
	if err != nil {
		slog.Error("Failed to update status and rendering", "id", id, "error", err)
	}
}
func updateURLAsCompleted(ctx context.Context, tx pgx.Tx, id int64, content *FetchedContent, status CrawlStatus) {
	query := `UPDATE urls SET status = $1, processed_at = NOW(), title = $2, description = $3, content = $4, rendering = $5 WHERE id = $6`
	_, err := tx.Exec(ctx, query, status, content.Title, content.Description, content.TextContent, SSR, id)
	if err != nil {
		slog.Error("Failed to update URL as completed", "id", id, "error", err)
	}
}
func resetStalledJobs(ctx context.Context) {
	query := `UPDATE urls SET status = CASE WHEN status = 'classifying' THEN 'pending_classification' WHEN status = 'crawling' THEN 'pending_crawl' END, locked_at = NULL WHERE status IN ('classifying', 'crawling') AND locked_at < NOW() - $1::interval`
	interval := fmt.Sprintf("%f minutes", AppConfig.JobTimeout.Minutes())
	res, err := dbpool.Exec(ctx, query, interval)
	if err != nil {
		slog.Error("Reaper: Failed to reset stalled jobs", "error", err)
	} else if res.RowsAffected() > 0 {
		slog.Warn("Reaper: Reset stalled jobs", "count", res.RowsAffected())
	}
}

func getEnv(key string, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}

func loadConfig() {
	AppConfig.DatabaseURL = getEnv("DATABASE_URL", "")
	AppConfig.MLApiURL = getEnv("ML_API_URL", "")
	if AppConfig.DatabaseURL == "" || AppConfig.MLApiURL == "" {
		slog.Error("DATABASE_URL and ML_API_URL must be set")
		os.Exit(1)
	}
	AppConfig.BatchSize, _ = strconv.Atoi(getEnv("BATCH_SIZE", "500"))
	AppConfig.MaxWorkers, _ = strconv.Atoi(getEnv("MAX_WORKERS", "50"))
	AppConfig.SleepInterval, _ = time.ParseDuration(getEnv("SLEEP_INTERVAL", "5s"))
	AppConfig.JobTimeout, _ = time.ParseDuration(getEnv("JOB_TIMEOUT", "15m"))
	AppConfig.FetchTimeout, _ = time.ParseDuration(getEnv("FETCH_TIMEOUT", "6s"))
	AppConfig.MaxUrlsPerNetloc, _ = strconv.Atoi(getEnv("MAX_URLS_PER_NETLOC", "130"))
	AppConfig.BatchWriteInterval, _ = time.ParseDuration(getEnv("BATCH_WRITE_INTERVAL", "10s"))
	AppConfig.BatchWriteQueueSize, _ = strconv.Atoi(getEnv("BATCH_WRITE_QUEUE_SIZE", "1000"))
	AppConfig.RestrictedTLDs = strings.Split(getEnv("RESTRICTED_TLDS", ".org,.edu"), ",")
	AppConfig.AllowedPathPrefixes = strings.Split(getEnv("ALLOWED_PATH_PREFIXES", "/blog"), ",")
	AppConfig.IgnoreExtensions = strings.Split(getEnv("IGNORE_EXTENSIONS", ".pdf,.jpg,.jpeg,.png,.gif,.zip,.rar,.exe,.mp3,.mp4,.avi,.mov,.dmg,.iso,.css,.js,.xml,.json,.gz,.tar,.tgz"), ",")
}
