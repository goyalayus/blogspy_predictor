package crawler

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
	"worker/packages/domain"

	"github.com/PuerkitoBio/goquery"
	"github.com/abadojack/whatlanggo"
)

type Crawler struct {
	client *http.Client
}

func New(timeout time.Duration) *Crawler {
	return &Crawler{
		client: &http.Client{Timeout: timeout},
	}
}

func (c *Crawler) FetchAndParseContent(ctx context.Context, rawURL string) (*domain.FetchedContent, error) {
	slog.Debug("Starting content fetch and parse", "url", rawURL)
	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		slog.Debug("Fetch returned bad status code", "url", rawURL, "status_code", resp.StatusCode)
		return nil, fmt.Errorf("bad status code: %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(strings.ToLower(contentType), "html") {
		slog.Debug("Content-Type is not HTML", "url", rawURL, "content_type", contentType)
		return &domain.FetchedContent{IsNonHTML: true, FinalURL: resp.Request.URL.String()}, nil
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

	content := &domain.FetchedContent{FinalURL: resp.Request.URL.String(), HTMLContent: htmlContent, GoqueryDoc: doc}

	if doc.Find("#root, #app, [data-reactroot]").Length() > 0 && len(strings.TrimSpace(doc.Find("body").Text())) < 250 {
		content.IsCSR = true
	}
	if doc.Find("template[data-dgst='BAILOUT_TO_CLIENT_SIDE_RENDERING']").Length() > 0 {
		content.IsCSR = true
	}

	content.Title = strings.TrimSpace(doc.Find("title").First().Text())
	if val, exists := doc.Find("meta[name='description']").Attr("content"); exists {
		content.Description = strings.TrimSpace(val)
	}

	doc.Find("script, style, noscript").Remove()
	re := strings.NewReplacer("\n", " ", "\t", " ", "\r", " ")
	content.TextContent = strings.Join(strings.Fields(re.Replace(doc.Text())), " ")

	var textSnippet string
	words := strings.Fields(content.TextContent)
	if len(words) > 100 {
		textSnippet = strings.Join(words[:100], " ")
	} else {
		textSnippet = content.TextContent
	}

	textForDetection := content.Title + " " + content.Description + " " + textSnippet

	if strings.TrimSpace(textForDetection) != "" {
		info := whatlanggo.Detect(textForDetection)
		content.Language = info.Lang.Iso6393()
	}

	if content.IsCSR {
		slog.Debug("Detected client-side rendering", "url", rawURL)
		return content, nil
	}

	return content, nil
}

// --- NEW FUNCTION START HERE ---

// AnalyzeParagraphs calculates the average word count of <p> tags in a document.
func (c *Crawler) AnalyzeParagraphs(doc *goquery.Document, threshold int) (isSubstantial bool, avgWordCount float64) {
	paragraphs := doc.Find("p")
	if paragraphs.Length() == 0 {
		return false, 0.0
	}

	var totalWords, paragraphCount int
	paragraphs.Each(func(i int, s *goquery.Selection) {
		wordCount := len(strings.Fields(s.Text()))
		if wordCount > 0 {
			totalWords += wordCount
			paragraphCount++
		}
	})

	if paragraphCount == 0 {
		return false, 0.0
	}

	avgWordCount = float64(totalWords) / float64(paragraphCount)
	return avgWordCount >= float64(threshold), avgWordCount
}

// --- NEW FUNCTION END HERE ---

func (c *Crawler) ExtractLinks(doc *goquery.Document, baseURL string, ignoreExtensions []string) []string {
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
		for _, ext := range ignoreExtensions {
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
	slog.Debug("Link extraction found links", "base_url", baseURL, "count", len(links))
	return links
}
