// language_test.go
package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/abadojack/whatlanggo"
)

func main() {
	// The URL to test
	targetURL := "https://goyalayus.github.io/"
	fmt.Printf("--- Testing language detection for: %s ---\n\n", targetURL)

	// 1. Fetch the content
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest("GET", targetURL, nil)
	if err != nil {
		log.Fatalf("FATAL: Could not create request: %v", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("FATAL: Failed to fetch URL: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Fatalf("FATAL: Received non-200 status code: %d", resp.StatusCode)
	}
	fmt.Println("✅ Content fetched successfully.")

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("FATAL: Could not read response body: %v", err)
	}

	// 2. Parse and clean the text (exactly as the worker does)
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(string(bodyBytes)))
	if err != nil {
		log.Fatalf("FATAL: Could not parse HTML: %v", err)
	}

	title := strings.TrimSpace(doc.Find("title").First().Text())
	description, _ := doc.Find("meta[name='description']").Attr("content")
	description = strings.TrimSpace(description)

	// CORRECTED: Selectors are now a single, comma-separated string.
	doc.Find("script, style, noscript").Remove()
	re := strings.NewReplacer("\n", " ", "\t", " ", "\r", " ")
	fullTextContent := strings.Join(strings.Fields(re.Replace(doc.Text())), " ")
	fmt.Println("✅ HTML parsed and text cleaned.")

	if len(fullTextContent) > 500 {
		fmt.Printf("\n--- Cleaned Text Sample (first 500 chars) ---\n%s...\n------------------------------------------\n\n", fullTextContent[:500])
	} else {
		fmt.Printf("\n--- Cleaned Text Sample ---\n%s\n------------------------------------------\n\n", fullTextContent)
	}

	// 3. Create the text sample for detection (exactly as the modified worker does)
	var textSnippet string
	words := strings.Fields(fullTextContent)
	if len(words) > 100 {
		textSnippet = strings.Join(words[:100], " ")
	} else {
		textSnippet = fullTextContent
	}
	textForDetection := title + " " + description + " " + textSnippet
	fmt.Printf("✅ Created detection sample of %d characters.\n", len(textForDetection))

	// 4. Run the language detection with detailed options
	options := whatlanggo.Options{}
	info := whatlanggo.DetectWithOptions(textForDetection, options)
	fmt.Println("\n--- Detection Results ---")
	fmt.Printf("Language Name:       %s\n", info.Lang.String())
	fmt.Printf("Language Code (ISO):  %s\n", info.Lang.Iso6393())
	fmt.Printf("Script:              %s\n", whatlanggo.Scripts[info.Script])
	fmt.Printf("Is Reliable?:        %t\n", info.IsReliable())
	fmt.Printf("Confidence Score:    %f\n", info.Confidence)
	fmt.Println("-------------------------")

	// 5. Explain the result
	if info.Lang.Iso6393() != "eng" && info.IsReliable() {
		fmt.Println("\nCONCLUSION: The library is CONFIDENTLY making an INCORRECT prediction.")
		fmt.Println("This is likely due to the specific combination of trigrams in your blog's text being statistically closer to its model for Spanish.")
	} else if info.Lang.Iso6393() != "eng" && !info.IsReliable() {
		fmt.Println("\nCONCLUSION: The library is making an INCORRECT prediction, but it is NOT CONFIDENT about it.")
		fmt.Println("The 'Is Reliable?' flag is false, which means we can use this to ignore the result and proceed.")
	} else {
		fmt.Println("\nCONCLUSION: The library correctly identified the language as English.")
	}
}
