import os
import sys
import io
from urllib.parse import urlparse
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

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
    data suitable for PostgreSQL's COPY command. This is memory-efficient.
    """
    print(f"Processing '{file_path}' with status '{status}'...")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            if has_header:
                next(f) # Skip the header line
            
            for line in f:
                url = line.strip()
                if not url:
                    continue
                
                try:
                    # Extract the network location (e.g., 'blog.kowalczyk.info')
                    parsed_url = urlparse(url)
                    netloc = parsed_url.netloc
                    if netloc:
                        # Yield a tab-separated string for COPY. This is the required format.
                        yield f"{url}\t{netloc}\t{status}\n"
                except Exception as e:
                    print(f"  [WARN] Skipping invalid URL '{url}': {e}", file=sys.stderr)

    except FileNotFoundError:
        print(f"  [ERROR] File not found: {file_path}. Please ensure it is in the project root.", file=sys.stderr)
        return


def bulk_load_data(conn, file_path, status, has_header=False):
    """
    Performs a high-performance bulk load using psycopg2's 'copy_from' method,
    which uses the PostgreSQL COPY protocol.
    """
    data_generator = generate_copy_data(file_path, status, has_header)
    
    # Use an in-memory string buffer to act as a temporary file for copy_from
    string_buffer = io.StringIO()
    string_buffer.writelines(data_generator)
    string_buffer.seek(0) # Rewind buffer to the beginning

    # If no valid URLs were found in the file, skip the database operation
    if string_buffer.tell() == 0 and len(string_buffer.getvalue()) == 0:
        print(f"No valid data generated from '{file_path}'. Skipping database operation.")
        return

    try:
        with conn.cursor() as cur:
            # This is the high-performance bulk insert command.
            cur.copy_from(string_buffer, 'urls', columns=('url', 'netloc', 'status'), sep='\t')
        conn.commit()
        print(f"✅ Successfully loaded data from '{file_path}'.")
    except Exception as e:
        print(f"  [ERROR] Failed to bulk load data from '{file_path}': {e}", file=sys.stderr)
        conn.rollback()


if __name__ == "__main__":
    conn = get_db_connection()
    
    # Step 1: Truncate tables for a completely clean slate.
    print("\n--- Wiping existing data ---")
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE urls, system_counters, url_content, url_edges RESTART IDENTITY CASCADE;")
        cur.execute("INSERT INTO system_counters (counter_name) VALUES ('pending_urls_count') ON CONFLICT DO NOTHING;")
    conn.commit()
    print("✅ Tables truncated and counters reset.")

    # Step 2: Bulk load the data from the two CSV files.
    print("\n--- Seeding new data from files ---")
    
    # Load personal blogs and mark them as 'pending_classification'
    # --- THIS IS THE CORRECTED LINE ---
    bulk_load_data(conn, 'searchmysite_urls.csv', 'pending_classification', has_header=True)
    
    # Load corporate sites and mark them as 'irrelevant'
    bulk_load_data(conn, 'corporate.csv', 'irrelevant', has_header=False)

    conn.close()
    print("\nDatabase seeding complete.")
