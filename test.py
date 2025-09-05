# seed_db.py
import os
import sys
import csv
import io
from urllib.parse import urlparse
import psycopg2

def get_db_connection():
    """Gets a database connection using the DATABASE_URL environment variable."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("FATAL: DATABASE_URL environment variable is not set.", file=sys.stderr)
        sys.exit(1)
    
    try:
        conn = psycopg2.connect(db_url)
        return conn
    except psycopg2.OperationalError as e:
        print(f"FATAL: Could not connect to the database: {e}", file=sys.stderr)
        sys.exit(1)

def generate_copy_data(file_path, status, has_header=False):
    """
    Reads a file of URLs, extracts the netloc, and yields tab-separated
    data suitable for PostgreSQL COPY.
    """
    print(f"Processing '{file_path}' with status '{status}'...")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            if has_header:
                next(f) # Skip header line
            
            for line in f:
                url = line.strip()
                if not url:
                    continue
                
                try:
                    parsed_url = urlparse(url)
                    netloc = parsed_url.netloc
                    if netloc:
                        # Yield a tab-separated string for COPY
                        yield f"{url}\t{netloc}\t{status}\n"
                except Exception as e:
                    print(f"  [WARN] Skipping invalid URL '{url}': {e}", file=sys.stderr)

    except FileNotFoundError:
        print(f"  [ERROR] File not found: {file_path}", file=sys.stderr)
        return


def bulk_load_data(conn, file_path, status, has_header=False):
    """Performs a bulk load using psycopg2's highly efficient copy_from."""
    data_generator = generate_copy_data(file_path, status, has_header)
    
    # Use an in-memory string buffer
    string_buffer = io.StringIO()
    string_buffer.writelines(data_generator)
    string_buffer.seek(0) # Rewind buffer to the beginning

    if string_buffer.tell() == 0 and len(string_buffer.getvalue()) == 0:
        print(f"No valid data generated from '{file_path}'. Skipping database operation.")
        return

    try:
        with conn.cursor() as cur:
            # Use copy_from for high-performance bulk insertion
            cur.copy_from(string_buffer, 'urls', columns=('url', 'netloc', 'status'))
        conn.commit()
        print(f"✅ Successfully loaded data from '{file_path}'.")
    except Exception as e:
        print(f"  [ERROR] Failed to bulk load data from '{file_path}': {e}", file=sys.stderr)
        conn.rollback()


if __name__ == "__main__":
    conn = get_db_connection()
    
    # Step 1: Truncate tables for a clean slate
    print("\n--- Wiping existing data ---")
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE urls, system_counters RESTART IDENTITY CASCADE;")
        cur.execute("INSERT INTO system_counters (counter_name) VALUES ('pending_urls_count') ON CONFLICT DO NOTHING;")
    conn.commit()
    print("✅ Tables truncated and counters reset.")

    # Step 2: Bulk load the data from CSV files
    print("\n--- Seeding new data ---")
    bulk_load_data(conn, 'searchmysite_urls.csv', 'pending_crawl', has_header=True)
    bulk_load_data(conn, 'corporate.csv', 'irrelevant', has_header=False)

    conn.close()
    print("\nDatabase seeding complete.")
