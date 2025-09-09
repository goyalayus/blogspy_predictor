import os
import sys
import psycopg2
from dotenv import load_dotenv
from urllib.parse import urlparse  # - NEW - Import the urlparse function

load_dotenv()

# --- CONFIGURATION ---
# Now you only need to change this one line!
TARGET_URL = "https://www.forbes.com/sites/zacharyfolk/2025/09/08/photo-allegedly-showing-birthday-message-to-epstein-signed-by-trump-given-to-congress/"
# ---------------------

def get_db_connection():
    """Gets a database connection using the DATABASE_URL environment variable."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("FATAL: DATABASE_URL environment variable is not set.", file=sys.stderr)
        sys.exit(1)
    
    try:
        print("Connecting to the database...")
        conn = psycopg2.connect(db_url)
        print("✅ Connection successful.")
        return conn
    except psycopg2.OperationalError as e:
        print(f"FATAL: Could not connect to the database: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    """Wipes the database and seeds a single URL for testing."""
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cur:
            # Step 1: Wipe all relevant tables for a completely clean slate
            print("\n--- Wiping existing data ---")
            cur.execute("TRUNCATE TABLE urls, url_content, url_edges RESTART IDENTITY CASCADE;")
            print("✅ Tables 'urls', 'url_content', and 'url_edges' truncated.")
            
            # Step 2: Insert our single target URL
            print(f"\n--- Seeding single URL: {TARGET_URL} ---")

            # - NEW - Automatically extract the netloc from the TARGET_URL
            try:
                parsed_url = urlparse(TARGET_URL)
                netloc = parsed_url.netloc
                if not netloc:
                    print(f"FATAL: Could not extract a valid netloc from URL: {TARGET_URL}", file=sys.stderr)
                    sys.exit(1)
                print(f"  -> Automatically extracted netloc: {netloc}")
            except Exception as e:
                print(f"FATAL: An error occurred while parsing the URL: {e}", file=sys.stderr)
                sys.exit(1)
            
            # Use the dynamically extracted netloc in the query
            cur.execute(
                "INSERT INTO urls (url, netloc, status) VALUES (%s, %s, %s);",
                (TARGET_URL, netloc, 'pending_classification')
            )
            print("✅ URL inserted successfully with status 'pending_classification'.")
            
        # Commit the transaction
        conn.commit()
        print("\nDatabase seeding complete. Changes have been committed.")
        
    except Exception as e:
        print(f"\n❌ An error occurred: {e}", file=sys.stderr)
        print("Rolling back any changes.")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
