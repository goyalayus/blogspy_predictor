# src/worker.py

from sqlalchemy.orm import Session
from sqlalchemy import func  # <-- NEW: Import func for COUNT
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import case, cast
from src.config import MODELS_DIR, REQUEST_HEADERS
from src.feature_engineering import extract_url_features, extract_structural_features, extract_content_features
from src.database import SessionLocal, URL, URLEdge, CrawlStatus, RenderingType
from src.utils import setup_logging
import sys
import pathlib
import joblib
import pandas as pd
import requests
import warnings
import scipy.sparse as sp
import psutil
import time
import os
import threading
import queue
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse
from collections import defaultdict
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from concurrent.futures import ThreadPoolExecutor, as_completed

project_root = pathlib.Path(__file__).parent.parent
sys.path.append(str(project_root))

MODEL_PATH = MODELS_DIR / 'lgbm_final_model.joblib'
BATCH_SIZE = 500
SLEEP_INTERVAL = 5
BATCH_WRITE_INTERVAL = 10

# --- NEW: Configuration for Crawling Rules ---
MAX_URLS_PER_NETLOC = 200
RESTRICTED_TLDS = ('.org', '.edu')
ALLOWED_PATH_PREFIXES = ('/blog')
# ---------------------------------------------

IGNORE_EXTENSIONS = (
    '.pdf', '.jpg', '.jpeg', '.png', '.gif', '.zip', '.rar', '.exe',
    '.mp3', '.mp4', '.avi', '.mov', '.dmg', '.iso', '.css', '.js',
    '.xml', '.json', '.gz', '.tar', '.tgz'
)

logger = setup_logging('worker')

thread_local_session = threading.local()
link_batch_queue = queue.Queue()


def get_session():
    if not hasattr(thread_local_session, "session"):
        thread_local_session.session = requests.Session()
    return thread_local_session.session


# --- Helper Functions (Unchanged) ---
def get_performance_metrics():
    return {"cpu_percent": psutil.cpu_percent(interval=0.1), "memory_rss_mb": round(psutil.Process().memory_info().rss / (1024 * 1024), 2)}


def check_for_csr(soup: BeautifulSoup) -> bool:
    if soup.find('div', id='root') or soup.find('div', id='app'):
        body_text = soup.body.get_text(
            strip=True, separator=' ') if soup.body else ''
        if len(body_text) < 250:
            return True
    if soup.find('template', attrs={'data-dgst': 'BAILOUT_TO_CLIENT_SIDE_RENDERING'}):
        return True
    return False


def fetch_and_parse_content(url: str) -> dict:
    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
    session = get_session()
    response = session.get(url, timeout=(
        2, 4), headers=REQUEST_HEADERS, allow_redirects=True)
    response.raise_for_status()
    content_type = response.headers.get('content-type', '').lower()
    if 'html' not in content_type:
        return {"is_non_html": True, "final_url": response.url}
    html = response.text
    soup = BeautifulSoup(html, 'lxml')
    is_csr = check_for_csr(soup)
    if is_csr:
        return {"is_csr": True, "soup": soup, "html_content": html, "final_url": response.url}
    title = (soup.find('title').get_text(strip=True)
             ) if soup.find('title') else None
    desc_tag = soup.find('meta', attrs={'name': 'description'})
    description = desc_tag['content'].strip(
    ) if desc_tag and desc_tag.get('content') else None
    for s in soup(['script', 'style']):
        s.decompose()
    text_content = ' '.join(soup.stripped_strings)
    return {"is_non_html": False, "is_csr": False, "html_content": html, "text_content": text_content, "title": title, "description": description, "soup": soup, "final_url": response.url}


def extract_links(html: str, base_url: str) -> list[str]:
    links = set()
    soup = BeautifulSoup(html, 'lxml')
    for a in soup.find_all('a', href=True):
        href = a['href'].strip()
        if not href or href.startswith(('#', 'mailto:', 'javascript:')):
            continue
        lower_href = href.lower()
        if any(lower_href.endswith(ext) for ext in IGNORE_EXTENSIONS):
            continue
        try:
            full_url = urljoin(base_url, href)
            parsed_url = urlparse(full_url)
            if parsed_url.scheme in ['http', 'https']:
                links.add(parsed_url._replace(fragment="").geturl())
        except Exception:
            continue
    return list(links)


# --- MODIFIED: This function now contains all the new filtering logic ---
def _perform_crawl_logic(db_session: Session, url_record: URL, content: dict):
    new_links = extract_links(content['html_content'], content["final_url"])
    new_links.sort()

    if not new_links:
        return

    # --- Step 1: Group links and prepare for efficient querying ---
    links_by_netloc = defaultdict(list)
    for link in new_links:
        if nloc := urlparse(link).netloc:
            links_by_netloc[nloc].append(link)

    all_netlocs_found = list(links_by_netloc.keys())

    # --- Step 2: Efficiently get existing link counts for all relevant netlocs ---
    netloc_count_query = db_session.query(
        URL.netloc, func.count(URL.id)
    ).filter(
        URL.netloc.in_(all_netlocs_found)
    ).group_by(URL.netloc)

    # Use defaultdict to simplify logic later
    netloc_counts = defaultdict(
        int, {netloc: count for netloc, count in netloc_count_query})

    # --- Step 3: Get existing URLs and domain decisions in batch ---
    existing_urls_q = db_session.query(URL.url).filter(URL.url.in_(new_links))
    existing_urls = {res.url for res in existing_urls_q}

    personal_statuses = {CrawlStatus.pending_crawl,
                         CrawlStatus.crawling, CrawlStatus.completed}
    domain_decisions_q = db_session.query(URL.netloc, URL.status).filter(
        URL.netloc.in_(all_netlocs_found),
        URL.status.in_(personal_statuses | {CrawlStatus.irrelevant})
    ).distinct(URL.netloc)
    domain_decisions = {res.netloc: res.status for res in domain_decisions_q}

    # --- Step 4: Apply rules and build lists for insertion ---
    links_to_add_to_db = []
    logged_full_domains = set()  # To prevent log spam for full domains

    for netloc, links in links_by_netloc.items():
        for link in links:
            # --- RULE 1: Check if the netloc is already full ---
            if netloc_counts[netloc] >= MAX_URLS_PER_NETLOC:
                if netloc not in logged_full_domains:
                    logger.info(
                        f"Netloc '{netloc}' is full ({netloc_counts[netloc]}/{MAX_URLS_PER_NETLOC}). Skipping new links.")
                    logged_full_domains.add(netloc)
                continue  # Skip this link and all others from this netloc

            # --- RULE 2: Check for restricted TLDs and allowed paths ---
            parsed_link = urlparse(link)
            if netloc.endswith(RESTRICTED_TLDS):
                path = parsed_link.path.lower()
                if not any(path.startswith(p) for p in ALLOWED_PATH_PREFIXES) and path not in ('', '/'):
                    logger.debug(
                        f"Skipping restricted link (TLD/path mismatch): {link}")
                    continue  # Skip this link

            # --- Existing check for duplicates ---
            if link in existing_urls:
                continue

            # Determine the status for the new link
            link_status = CrawlStatus.pending_classification
            if netloc == urlparse(url_record.url).netloc:
                link_status = CrawlStatus.pending_crawl
            elif netloc in domain_decisions:
                decision = domain_decisions[netloc]
                if decision == CrawlStatus.irrelevant:
                    link_status = CrawlStatus.irrelevant
                elif decision in personal_statuses:
                    link_status = CrawlStatus.pending_crawl

            links_to_add_to_db.append(
                {"url": link, "netloc": netloc, "status": link_status})

            # CRUCIAL: Increment the local counter to respect the limit within this batch
            netloc_counts[netloc] += 1

    # --- The critical change: Put the work onto the queue ---
    if links_to_add_to_db:
        link_batch_queue.put((url_record.id, links_to_add_to_db))
        logger.info(
            f"✅ Queued {len(links_to_add_to_db)} new URLs for batch insertion (from {url_record.url})")


# --- Task Processing Functions (Unchanged) ---
def process_classification_task(db_session: Session, url_record: URL, artifact: dict):
    try:
        content = fetch_and_parse_content(url_record.url)
        if content.get("is_non_html"):
            url_record.status = CrawlStatus.irrelevant
            url_record.error_message = "Content-Type was not HTML"
            return
        if content.get("is_csr"):
            url_record.status = CrawlStatus.irrelevant
            url_record.rendering = RenderingType.CSR
            url_record.error_message = "Detected Client-Side Rendering"
            return

        url_record.rendering = RenderingType.SSR
        df = pd.DataFrame([{**content, "url": content["final_url"]}])
        txt_feat = artifact['vectorizer'].transform(
            df["text_content"].fillna(""))
        num_feat_df = pd.concat([extract_url_features(df['url']), extract_structural_features(
            df['html_content']), extract_content_features(df['text_content'])], axis=1)
        num_feat = sp.csr_matrix(num_feat_df.to_numpy(dtype="float32"))
        features = sp.hstack([txt_feat, num_feat], format="csr")
        is_personal_blog = (artifact['model'].predict(features)[0] > 0.5)

        url_record.processed_at = datetime.now(timezone.utc)
        url_record.title = content.get('title')
        url_record.description = content.get('description')
        url_record.content = content.get('text_content')

        if is_personal_blog:
            _perform_crawl_logic(db_session, url_record, content)
            url_record.status = CrawlStatus.completed
        else:
            url_record.status = CrawlStatus.irrelevant

    except Exception as e:
        logger.error(
            f"Failed to classify {url_record.url}: {e}", exc_info=False)
        url_record.status = CrawlStatus.failed
        url_record.error_message = str(e)


def process_crawl_task(db_session: Session, url_record: URL):
    try:
        content = fetch_and_parse_content(url_record.url)
        if content.get("is_non_html"):
            url_record.status = CrawlStatus.completed
            url_record.error_message = "Content-Type was not HTML"
            return
        if content.get("is_csr"):
            url_record.status = CrawlStatus.completed
            url_record.rendering = RenderingType.CSR
            url_record.error_message = "Detected CSR during crawl"
            return

        _perform_crawl_logic(db_session, url_record, content)

        url_record.status = CrawlStatus.completed
        url_record.processed_at = datetime.now(timezone.utc)
        url_record.title = content.get('title')
        url_record.description = content.get('description')
        url_record.content = content.get('text_content')

    except Exception as e:
        logger.error(f"Failed to crawl {url_record.url}: {e}", exc_info=False)
        url_record.status = CrawlStatus.failed
        url_record.error_message = str(e)


# --- Bulletproof Thread Wrapper (Unchanged) ---
def run_task_in_thread(task_func, job_id: int, *args):
    db_session = SessionLocal()
    try:
        job_to_process = db_session.query(URL).filter(URL.id == job_id).first()
        if not job_to_process:
            logger.warning(f"Job with ID {job_id} not found in thread.")
            return
        task_func(db_session, job_to_process, *args)
        db_session.commit()
    except Exception as e:
        db_session.rollback()
        logger.error(
            f"Transaction failed for job ID {job_id}. Rolling back. Error: {e}", exc_info=False)
    finally:
        db_session.close()


# --- "Reaper" Function for Fault Tolerance (Unchanged) ---
def reset_stalled_jobs(db_session: Session):
    try:
        timeout_interval = 15
        smart_reset_status = case(
            (URL.status == 'classifying', cast(
                'pending_classification', URL.status.type)),
            (URL.status == 'crawling',    cast('pending_crawl', URL.status.type)),
        )
        stalled_jobs_query = db_session.query(URL).filter(
            URL.status.in_(['classifying', 'crawling']),
            URL.locked_at < (datetime.now(timezone.utc) -
                             pd.Timedelta(minutes=timeout_interval))
        )
        rows_reset = stalled_jobs_query.update(
            {'status': smart_reset_status, 'locked_at': None},
            synchronize_session=False
        )
        if rows_reset > 0:
            db_session.commit()
            logger.warning(
                f"Reaper: Reset {rows_reset} stalled jobs to their pending state.")
        else:
            db_session.rollback()
    except Exception as e:
        logger.error(
            f"Reaper: Failed to reset stalled jobs: {e}", exc_info=True)
        db_session.rollback()


# --- Database Writer Thread (Unchanged) ---
def database_writer_thread():
    """
    A dedicated thread that runs in the background, consuming links from the
    queue and performing massive, efficient bulk writes to the database.
    """
    while True:
        time.sleep(BATCH_WRITE_INTERVAL)
        batch_items = []
        while not link_batch_queue.empty():
            try:
                batch_items.append(link_batch_queue.get_nowait())
            except queue.Empty:
                break
        if not batch_items:
            continue
        db = SessionLocal()
        try:
            all_link_records = []
            for _, link_list in batch_items:
                all_link_records.extend(link_list)
            if not all_link_records:
                db.close()
                continue
            logger.info(
                f"DB-Writer: Processing batch of {len(all_link_records)} links from {len(batch_items)} sources.")
            url_insert_stmt = pg_insert(URL).values(
                all_link_records).on_conflict_do_nothing(index_elements=['url'])
            db.execute(url_insert_stmt)
            all_urls_in_batch = {record['url'] for record in all_link_records}
            id_results = db.query(URL.id, URL.url).filter(
                URL.url.in_(all_urls_in_batch)).all()
            url_to_id_map = {url: url_id for url_id, url in id_results}
            edge_values_to_insert = []
            for source_id, link_list in batch_items:
                for link_record in link_list:
                    dest_url = link_record['url']
                    if dest_url in url_to_id_map:
                        dest_id = url_to_id_map[dest_url]
                        edge_values_to_insert.append(
                            {"source_url_id": source_id, "dest_url_id": dest_id})
            if edge_values_to_insert:
                edge_insert_stmt = pg_insert(URLEdge).values(
                    edge_values_to_insert).on_conflict_do_nothing()
                db.execute(edge_insert_stmt)
            db.commit()
            logger.info(
                f"DB-Writer: Successfully committed batch. {len(edge_values_to_insert)} new edges created.")
        except Exception as e:
            logger.error(
                f"DB-Writer: An error occurred during batch write. Rolling back. Error: {e}", exc_info=True)
            db.rollback()
        finally:
            db.close()


# --- Main Loop (Unchanged) ---
def main():
    logger.info("--- Starting BlogSpy Worker ---")
    try:
        artifact = joblib.load(MODEL_PATH)
        logger.info("Model loaded successfully.")
    except FileNotFoundError:
        logger.error(f"Model file not found at {MODEL_PATH}. Exiting.")
        sys.exit(1)

    cpu_cores = os.cpu_count() or 1
    max_workers = min(BATCH_SIZE * 2, cpu_cores * 5)
    logger.info(
        f"Initialized with BATCH_SIZE={BATCH_SIZE} and max_workers={max_workers}.")

    writer_thread = threading.Thread(
        target=database_writer_thread, daemon=True)
    writer_thread.start()
    logger.info("Database writer thread started.")

    while True:
        logger.debug("Starting new worker cycle", extra={
                     "event": "cycle_start", "performance": get_performance_metrics()})
        db = SessionLocal()
        job_processed_in_cycle = False
        try:
            reset_stalled_jobs(db)
            for job_type, status_from, status_to, process_func, args in [
                ("crawling", CrawlStatus.pending_crawl,
                 CrawlStatus.crawling, process_crawl_task, []),
                ("classification", CrawlStatus.pending_classification,
                 CrawlStatus.classifying, process_classification_task, [artifact])
            ]:
                job_ids_query = db.query(URL.id).filter(
                    URL.status == status_from).with_for_update(skip_locked=True).limit(BATCH_SIZE)
                job_ids = [id_tuple[0] for id_tuple in job_ids_query.all()]
                if job_ids:
                    job_processed_in_cycle = True
                    db.query(URL).filter(URL.id.in_(job_ids)).update(
                        {'status': status_to,
                            'locked_at': datetime.now(timezone.utc)},
                        synchronize_session=False
                    )
                    db.commit()
                    logger.info(
                        f"Locked and dispatched {len(job_ids)} {job_type} jobs.")
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        future_to_job_id = {executor.submit(
                            run_task_in_thread, process_func, job_id, *args): job_id for job_id in job_ids}
                        jobs_done_in_batch = 0
                        for future in as_completed(future_to_job_id):
                            jobs_done_in_batch += 1
                            try:
                                future.result()
                                if jobs_done_in_batch % 20 == 0:
                                    logger.info(
                                        f"Batch progress: {jobs_done_in_batch}/{len(job_ids)} {job_type} jobs finished.")
                            except Exception:
                                pass
                    logger.info(
                        f"Batch of {len(job_ids)} {job_type} jobs complete.")
            if not job_processed_in_cycle:
                logger.info(
                    f"No pending jobs in this cycle. Sleeping for {SLEEP_INTERVAL} seconds.")
                time.sleep(SLEEP_INTERVAL)
        except Exception as e:
            logger.error(
                f"An unexpected error occurred in the main loop: {e}", exc_info=True)
            if db.is_active:
                db.rollback()
            time.sleep(SLEEP_INTERVAL)
        finally:
            if db.is_active:
                db.close()


if __name__ == "__main__":
    main()
