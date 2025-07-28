// Package domain
package domain

import "github.com/PuerkitoBio/goquery"

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

// Still used for the link ingestion writer
type LinkBatch struct {
	SourceURLID int64
	NewLinks    []NewLink
}

type NewLink struct {
	URL, Netloc string
	Status      CrawlStatus
}

// NEW: Struct for enqueuing status updates for failed/irrelevant jobs
type StatusUpdateResult struct {
	ID        int64
	Status    CrawlStatus
	Rendering RenderingType // Used for CSR case
	ErrorMsg  string
}

// NEW: Struct for enqueuing content for completed jobs
type ContentInsertResult struct {
	ID          int64
	Title       string
	Description string
	TextContent string
	Rendering   RenderingType
}
