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
	Language                                               string // NEW: Detected language code (e.g., "eng")
	GoqueryDoc                                             *goquery.Document
}

type LinkBatch struct {
	SourceURLID int64
	NewLinks    []NewLink
}

type NewLink struct {
	URL, Netloc string
	Status      CrawlStatus
}

type StatusUpdateResult struct {
	ID        int64
	Status    CrawlStatus
	Rendering RenderingType
	ErrorMsg  string
}

type ContentInsertResult struct {
	ID          int64
	Title       string
	Description string
	TextContent string
	Rendering   RenderingType
}
