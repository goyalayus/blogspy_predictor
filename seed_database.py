from src.database import SessionLocal, URL, CrawlStatus
from sqlalchemy.dialects.postgresql import insert as pg_insert
import csv
import sys
import pathlib
from urllib.parse import urlparse
from datetime import datetime, timezone

# Ensure the script can find the src modules
project_root = pathlib.Path(__file__).parent
sys.path.append(str(project_root))


# --- CONFIGURATION ---
PERSONAL_BLOGS_FILE = 'searchmysite_urls.csv'
CORPORATE_SITES_FILE = 'corporate.csv'


def prepare_records(filepath: str, classification: str, status: CrawlStatus) -> list[dict]:
    """Reads a CSV file and prepares a list of dictionaries for database insertion."""
    records = []
    print("Processing file: {0}".format(filepath))
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # Skip header row
            for row in reader:
                if not row:
                    continue
                url = row[0].strip()
                try:
                    # Ensure URL is valid and extract netloc
                    parsed = urlparse(url)
                    if not all([parsed.scheme, parsed.netloc]):
                        print("  Skipping invalid URL: {0}".format(url))
                        continue

                    netloc = parsed.netloc

                    records.append({
                        "url": url,
                        "netloc": netloc,
                        "status": status,
                        "classification": classification,
                        "confidence": 1.0,  # We are 100% confident in these
                        "processed_at": datetime.now(timezone.utc)
                    })
                except Exception as e:
                    print("  Error processing URL '{0}': {1}".format(url, e))
                    continue
    except FileNotFoundError:
        print("!!! File not found: {0}. Please ensure it is in the project root.".format(
            filepath))

    print("  -> Found {0} valid URLs.".format(len(records)))
    return records


def main():
    """
    Main function to seed the database from the provided CSV files.
    """
    print("--- Starting Database Seeding Script ---")

    # Prepare records from both files
    personal_blog_records = prepare_records(
        filepath=PERSONAL_BLOGS_FILE,
        classification="PERSONAL_BLOG",
        status=CrawlStatus.pending_crawl  # Ready to be crawled for links
    )

    corporate_site_records = prepare_records(
        filepath=CORPORATE_SITES_FILE,
        classification="CORPORATE_SEO",
        status=CrawlStatus.irrelevant  # No need to crawl
    )

    all_records = personal_blog_records + corporate_site_records

    if not all_records:
        print("\nNo records to insert. Exiting.")
        return

    print(
        "\nAttempting to insert/update a total of {0} records...".format(len(all_records)))

    db = SessionLocal()
    try:
        stmt = pg_insert(URL).values(all_records)

        # If a URL already exists, update it with our high-confidence data
        update_stmt = stmt.on_conflict_do_update(
            index_elements=['url'],
            set_={
                'netloc': stmt.excluded.netloc,
                'status': stmt.excluded.status,
                'classification': stmt.excluded.classification,
                'confidence': stmt.excluded.confidence,
                'processed_at': stmt.excluded.processed_at,
            }
        )

        db.execute(update_stmt)
        db.commit()
        print("\n✅ Success! Database has been seeded.")

    except Exception as e:
        print("\n❌ An error occurred during database insertion.")
        print(e)
        db.rollback()
    finally:
        db.close()


if __name__ == "__main__":
    main()
