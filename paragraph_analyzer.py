import argparse
import requests
from bs4 import BeautifulSoup
import sys
import json

# --- Configuration ---
# The number of words a paragraph must average to be considered "blog-like".
WORD_COUNT_THRESHOLD = 30

def fetch_and_parse(url: str) -> BeautifulSoup | None:
    """
    Fetches the content of a URL and returns a BeautifulSoup object.
    Handles common errors gracefully.
    """
    print(f"\n[1] Fetching content from: {url}")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        # Raise an exception for bad status codes (like 404 or 500)
        response.raise_for_status()
        print("✅ Fetch successful.")
        return BeautifulSoup(response.text, 'html.parser')
    except requests.RequestException as e:
        print(f"❌ ERROR: Could not fetch URL. Reason: {e}", file=sys.stderr)
        return None

def analyze_paragraphs(soup: BeautifulSoup) -> tuple[float, int, dict]:
    """
    Analyzes all <p> tags in the document.
    
    Returns:
        - The average word count of non-empty paragraphs.
        - The total number of <p> tags found.
        - A dictionary detailing the word count of each paragraph.
    """
    print("[2] Analyzing paragraphs...")
    
    # Find all paragraph tags
    paragraphs = soup.find_all('p')
    
    if not paragraphs:
        print("⚠️ No <p> tags found on this page.")
        return 0.0, 0, {}

    paragraph_details = {}
    word_counts = []

    for i, p_tag in enumerate(paragraphs):
        # Get text, split into words, and count them
        text = p_tag.get_text()
        word_count = len(text.split())
        
        # Store the word count for our detailed log
        paragraph_details[f"paragraph_{i+1}"] = word_count
        
        # Only include paragraphs with actual words in the average calculation
        if word_count > 0:
            word_counts.append(word_count)

    if not word_counts:
        print("⚠️ Found <p> tags, but they were all empty.")
        return 0.0, len(paragraphs), paragraph_details
    
    # Calculate the average
    average_words = sum(word_counts) / len(word_counts)
    
    print(f"✅ Analysis complete. Found {len(paragraphs)} total <p> tags.")
    return average_words, len(paragraphs), paragraph_details

def main():
    parser = argparse.ArgumentParser(
        description="A simple script to determine if a URL is a blog based on average paragraph length."
    )
    parser.add_argument("url", help="The website URL to analyze.")
    args = parser.parse_args()

    soup = fetch_and_parse(args.url)
    if not soup:
        sys.exit(1) # Exit if fetching failed

    avg_words, total_p_tags, details = analyze_paragraphs(soup)

    # --- Make the final prediction ---
    is_blog = avg_words >= WORD_COUNT_THRESHOLD

    # --- Print the Report ---
    print("\n" + "="*40)
    print("      Paragraph Analysis Report")
    print("="*40)
    
    print("\n--- PREDICTION ---")
    if is_blog:
        print("Result: ✅ Likely a Blog")
    else:
        print("Result: ❌ Unlikely to be a Blog")
    
    print("\n--- DIAGNOSTICS ---")
    print(f"URL Analyzed:        {args.url}")
    print(f"Decision Threshold:  > {WORD_COUNT_THRESHOLD} words")
    print(f"Average Word Count:  {avg_words:.2f} words (for non-empty paragraphs)")
    print(f"Total <p> Tags Found: {total_p_tags}")
    
    print("\n--- PARAGRAPH DETAILS (JSON) ---")
    # Using json.dumps for pretty printing the dictionary
    print(json.dumps(details, indent=2))
    
    print("\n" + "="*40)

if __name__ == "__main__":
    main()
