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

	// Basic CSR detection
	if doc.Find("#root, #app, [data-reactroot]").Length() > 0 && len(strings.TrimSpace(doc.Find("body").Text())) < 250 {
		content.IsCSR = true
	}
	// Next.js specific CSR bailout indicator
	if doc.Find("template[data-dgst='BAILOUT_TO_CLIENT_SIDE_RENDERING']").Length() > 0 {
		content.IsCSR = true
	}

	// Extract content before language detection.
	content.Title = strings.TrimSpace(doc.Find("title").First().Text())
	if val, exists := doc.Find("meta[name='description']").Attr("content"); exists {
		content.Description = strings.TrimSpace(val)
	}

	// --- NEW: Language Detection Logic ---
	textForDetection := content.Title + " " + content.Description
	// Fallback to text content if title/desc are empty
	if strings.TrimSpace(textForDetection) == "" {
		doc.Find("script, style, noscript").Remove()
		re := strings.NewReplacer("\n", " ", "\t", " ", "\r", " ")
		content.TextContent = strings.Join(strings.Fields(re.Replace(doc.Text())), " ")
		// Use a small part of the body text for detection as a fallback
		if len(content.TextContent) > 500 {
			textForDetection = content.TextContent[:500]
		} else {
			textForDetection = content.TextContent
		}
	}

	if textForDetection != "" {
		info := whatlanggo.Detect(textForDetection)
		content.Language = info.Lang.Iso6393() // e.g., "eng", "deu"
	}
	// End of new logic

	if content.IsCSR {
		slog.Debug("Detected client-side rendering", "url", rawURL)
		return content, nil
	}

	// If not CSR and text content wasn't already extracted for the fallback, extract it now.
	if content.TextContent == "" {
		doc.Find("script, style, noscript").Remove()
		re := strings.NewReplacer("\n", " ", "\t", " ", "\r", " ")
		content.TextContent = strings.Join(strings.Fields(re.Replace(doc.Text())), " ")
	}

	return content, nil
}

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
