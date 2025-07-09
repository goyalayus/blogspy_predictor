# src/worker.py (Final Bulletproof Version)

from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
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
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse
from collections import defaultdict
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from concurrent.futures import ThreadPoolExecutor, as_completed

project_root = pathlib.Path(__file__).parent.parent
sys.path.append(str(project_root))

MODEL_PATH = MODELS_DIR / 'lgbm_final_model.joblib'
BATCH_SIZE = 100
SLEEP_INTERVAL = 5

IGNORE_EXTENSIONS = (
    '.pdf', '.jpg', '.jpeg', '.png', '.gif', '.zip', '.rar', '.exe',
    '.mp3', '.mp4', '.avi', '.mov', '.dmg', '.iso', '.css', '.js',
    '.xml', '.json', '.gz', '.tar', '.tgz'
)

logger = setup_logging('worker')

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
    response = requests.get(url, timeout=(
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


def bulk_insert_with_status(db: Session, records: list[dict]):
    if not records:
        return
    stmt = pg_insert(URL).values(
        records).on_conflict_do_nothing(index_elements=['url'])
    db.execute(stmt)

# --- Task Processing Functions (NO LONGER COMMIT) ---


def process_classification_task(url_record: URL, artifact: dict):
    # This function now only sets the status on the object.
    # The commit is handled by the run_task_in_thread wrapper.
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
        url_record.status = CrawlStatus.pending_crawl if is_personal_blog else CrawlStatus.irrelevant
    except Exception as e:
        logger.error(
            f"Failed to classify {url_record.url}: {e}", exc_info=False)
        url_record.status = CrawlStatus.failed
        url_record.error_message = str(e)


def process_crawl_task(db_session: Session, url_record: URL):
    # This function now only sets the status on the object.
    # It still needs the db_session to query for existing links.
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
        new_links = extract_links(
            content['html_content'], content["final_url"])
        if new_links:
            # This part still needs the session to perform its logic
            links_by_netloc = defaultdict(list)
            for link in new_links:
                if nloc := urlparse(link).netloc:
                    links_by_netloc[nloc].append(link)
            existing_urls_q = db_session.query(
                URL.url).filter(URL.url.in_(new_links))
            existing_urls = {res.url for res in existing_urls_q}
            personal_statuses = {CrawlStatus.pending_crawl,
                                 CrawlStatus.crawling, CrawlStatus.completed}
            domain_decisions_q = db_session.query(URL.netloc, URL.status).filter(URL.netloc.in_(
                links_by_netloc.keys()), URL.status.in_(personal_statuses | {CrawlStatus.irrelevant})).distinct(URL.netloc)
            domain_decisions = {
                res.netloc: res.status for res in domain_decisions_q}
            to_insert_irrelevant, to_insert_pending_crawl, to_insert_pending_classification = [], [], []
            for netloc, links in links_by_netloc.items():
                for link in links:
                    if link in existing_urls:
                        continue
                    if netloc == url_record.netloc:
                        to_insert_pending_crawl.append(
                            {"url": link, "netloc": netloc, "status": CrawlStatus.pending_crawl})
                    elif netloc in domain_decisions:
                        decision = domain_decisions[netloc]
                        if decision == CrawlStatus.irrelevant:
                            to_insert_irrelevant.append(
                                {"url": link, "netloc": netloc, "status": CrawlStatus.irrelevant})
                        elif decision in personal_statuses:
                            to_insert_pending_crawl.append(
                                {"url": link, "netloc": netloc, "status": CrawlStatus.pending_crawl})
                    else:
                        to_insert_pending_classification.append(
                            {"url": link, "netloc": netloc, "status": CrawlStatus.pending_classification})
            bulk_insert_with_status(db_session, to_insert_irrelevant)
            bulk_insert_with_status(
                db_session, to_insert_pending_classification)
            bulk_insert_with_status(db_session, to_insert_pending_crawl)
            db_session.flush()
            all_inserted_links = [r['url'] for r in to_insert_irrelevant +
                                  to_insert_pending_crawl + to_insert_pending_classification]
            if all_inserted_links:
                dest_ids_q = db_session.query(URL.id).filter(
                    URL.url.in_(all_inserted_links))
                dest_ids = [res.id for res in dest_ids_q]
                edge_values = [{"source_url_id": url_record.id,
                                "dest_url_id": dest_id} for dest_id in dest_ids]
                if edge_values:
                    edge_stmt = pg_insert(URLEdge).values(
                        edge_values).on_conflict_do_nothing()
                    db_session.execute(edge_stmt)
        url_record.status = CrawlStatus.completed
        url_record.processed_at = datetime.now(timezone.utc)
        url_record.title = content.get('title')
        url_record.description = content.get('description')
    except Exception as e:
        logger.error(f"Failed to crawl {url_record.url}: {e}", exc_info=False)
        url_record.status = CrawlStatus.failed
        url_record.error_message = str(e)

# --- Bulletproof Thread Wrapper ---


def run_task_in_thread(task_func, job_id: int, *args):
    db_session = SessionLocal()
    try:
        job_to_process = db_session.query(URL).filter(URL.id == job_id).first()
        if not job_to_process:
            logger.warning(
                f"Job with ID {job_id} not found in thread, might have been processed or deleted.")
            return

        # The actual work happens here. The task function modifies the job_to_process object.
        # Note: process_crawl_task needs the session for its sub-queries.
        if task_func == process_crawl_task:
            task_func(db_session, job_to_process, *args)
        else:
            task_func(job_to_process, *args)

        # If the function completes without raising an exception, commit the changes.
        db_session.commit()

    except Exception as e:
        # If ANY exception occurs (in the task or during the commit), rollback everything.
        db_session.rollback()
        # Log the top-level error that caused the entire transaction to fail.
        logger.error(
            f"Transaction failed for job ID {job_id}. Rolling back. Error: {e}", exc_info=True)
    finally:
        # Always close the session to return the connection to the pool.
        db_session.close()


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

    while True:
        logger.debug("Starting new worker cycle", extra={
                     "event": "cycle_start", "performance": get_performance_metrics()})
        db = SessionLocal()
        job_processed_in_cycle = False
        try:
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
                        {'status': status_to}, synchronize_session=False)
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
                                if jobs_done_in_batch % 10 == 0:
                                    logger.info(
                                        f"Batch progress: {jobs_done_in_batch}/{len(job_ids)} {job_type} jobs finished.")
                            except Exception:
                                # Error is already logged in detail by run_task_in_thread.
                                # We just need to catch it here so as_completed doesn't stop.
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
