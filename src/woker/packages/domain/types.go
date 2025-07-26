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

type LinkBatch struct {
	SourceURLID int64
	NewLinks    []NewLink
}

type NewLink struct {
	URL, Netloc string
	Status      CrawlStatus
}
