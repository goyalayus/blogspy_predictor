# src/worker.py

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
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse
from collections import defaultdict
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

project_root = pathlib.Path(__file__).parent.parent
sys.path.append(str(project_root))

MODEL_PATH = MODELS_DIR / 'lgbm_final_model.joblib'
BATCH_SIZE = 5
SLEEP_INTERVAL = 10
PERSONAL_BLOG_LABEL = "PERSONAL_BLOG"

logger = setup_logging('worker')

# ... (Helper functions get_performance_metrics, fetch_and_parse_content, extract_links, bulk_insert_with_status remain the same) ...


def get_performance_metrics():
    return {"cpu_percent": psutil.cpu_percent(interval=0.1), "memory_rss_mb": round(psutil.Process().memory_info().rss / (1024 * 1024), 2)}


def fetch_and_parse_content(url: str) -> dict:
    response = requests.get(url, timeout=10, headers=REQUEST_HEADERS)
    response.raise_for_status()
    html = response.text
    soup = BeautifulSoup(html, 'lxml')
    is_csr = check_for_csr(soup)
    if is_csr:
        return {"is_csr": True, "soup": soup, "html_content": html}
    title = (soup.find('title').get_text(strip=True)
             ) if soup.find('title') else None
    desc = soup.find('meta', attrs={'name': 'description'})
    description = desc['content'].strip(
    ) if desc and desc.get('content') else None
    for s in soup(['script', 'style']):
        s.decompose()
    text_content = ' '.join(soup.stripped_strings)
    return {"is_csr": False, "html_content": html, "text_content": text_content, "title": title, "description": description, "soup": soup}


def check_for_csr(soup: BeautifulSoup) -> bool:
    if soup.find('div', id='root') or soup.find('div', id='app'):
        body_text = soup.body.get_text(
            strip=True, separator=' ') if soup.body else ''
        if len(body_text) < 250:
            return True
    if soup.find('template', attrs={'data-dgst': 'BAILOUT_TO_CLIENT_SIDE_RENDERING'}):
        return True
    return False


def extract_links(html, base_url):
    links = set()
    for a in BeautifulSoup(html, 'lxml').find_all('a', href=True):
        href = a['href'].strip()
        if not href or href.startswith(('#', 'mailto:', 'javascript:')):
            continue
        try:
            full_url = urljoin(base_url, href)
            if urlparse(full_url).scheme in ['http', 'https']:
                links.add(full_url)
        except:
            continue
    return list(links)


def bulk_insert_with_status(db: Session, records: list[dict]):
    if not records:
        return
    stmt = pg_insert(URL).values(
        records).on_conflict_do_nothing(index_elements=['url'])
    db.execute(stmt)


def process_classification_task(db, url_record: URL, artifact: dict):
    # This function remains the same and is correct.
    logger.info(f"[CLASSIFY] Starting: {url_record.url}")
    try:
        content = fetch_and_parse_content(url_record.url)
        if content.get("is_csr"):
            logger.warning(
                f"⚠️ Detected CSR for {url_record.url}. Marking as irrelevant.")
            url_record.status = CrawlStatus.irrelevant
            url_record.rendering = RenderingType.CSR
            url_record.error_message = "Detected Client-Side Rendering"
            db.commit()
            return
        url_record.rendering = RenderingType.SSR
        df = pd.DataFrame([{**content, "url": url_record.url}])
        txt_feat = artifact['vectorizer'].transform(
            df["text_content"].fillna(""))
        num_feat_df = pd.concat([extract_url_features(df['url']), extract_structural_features(
            df['html_content']), extract_content_features(df['text_content'])], axis=1)
        num_feat = sp.csr_matrix(num_feat_df.to_numpy(dtype="float32"))
        features = sp.hstack([txt_feat, num_feat], format="csr")
        is_personal_blog = (artifact['model'].predict(
            features) > 0.5).astype(int)[0]
        url_record.processed_at = datetime.now(timezone.utc)
        url_record.title = content.get('title')
        url_record.description = content.get('description')
        if is_personal_blog:
            url_record.status = CrawlStatus.pending_crawl
            url_record.content = content['text_content']
            logger.info(
                f"✅ Classified as Personal Blog. Queued for crawling: {url_record.url}")
        else:
            url_record.status = CrawlStatus.irrelevant
            logger.info(f"❌ Classified as Irrelevant: {url_record.url}")
    except Exception as e:
        logger.error(
            f"Failed to classify {url_record.url}: {e}", exc_info=False)
        url_record.status = CrawlStatus.failed
        url_record.error_message = str(e)
    db.commit()


def process_crawl_task(db, url_record: URL):
    logger.info(f"[CRAWL] Starting: {url_record.url}")
    log_extra = {"event": "crawl_task",
                 "url_id": url_record.id, "url": url_record.url}
    try:
        content = fetch_and_parse_content(url_record.url)
        if content.get("is_csr"):
            logger.warning(
                f"⚠️ Detected CSR for {url_record.url}. Skipping crawl.")
            url_record.status = CrawlStatus.completed
            url_record.rendering = RenderingType.CSR
            url_record.error_message = "Detected CSR during crawl"
            db.commit()
            return

        url_record.rendering = RenderingType.SSR
        url_record.title = content.get('title')
        url_record.description = content.get('description')
        url_record.content = content.get('text_content')

        new_links = extract_links(content['html_content'], url_record.url)

        if new_links:
            # ... (fast-path logic for inserting into 'urls' table is the same) ...
            links_by_netloc = defaultdict(list)
            for link in new_links:
                if nloc := urlparse(link).netloc:
                    links_by_netloc[nloc].append(link)
            existing_urls = {u.url for u in db.query(
                URL.url).filter(URL.url.in_(new_links))}
            personal_statuses = {CrawlStatus.pending_crawl,
                                 CrawlStatus.crawling, CrawlStatus.completed}
            domain_decisions_q = db.query(URL.netloc, URL.status).filter(URL.netloc.in_(links_by_netloc.keys(
            )), URL.status.in_(personal_statuses | {CrawlStatus.irrelevant})).distinct(URL.netloc)
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

            bulk_insert_with_status(db, to_insert_irrelevant)
            bulk_insert_with_status(db, to_insert_pending_classification)
            bulk_insert_with_status(db, to_insert_pending_crawl)
            db.flush()  # Ensure the new URLs are available for querying IDs

            # *** THE FIX IS HERE: Create the graph edges ***
            all_inserted_links = [r['url'] for r in to_insert_irrelevant +
                                  to_insert_pending_crawl + to_insert_pending_classification]
            if all_inserted_links:
                dest_ids_q = db.query(URL.id).filter(
                    URL.url.in_(all_inserted_links))
                dest_ids = [res.id for res in dest_ids_q]
                edge_values = [{"source_url_id": url_record.id,
                                "dest_url_id": dest_id} for dest_id in dest_ids]

                if edge_values:
                    edge_stmt = pg_insert(URLEdge).values(
                        edge_values).on_conflict_do_nothing()
                    db.execute(edge_stmt)
                    log_extra['edges_created'] = len(edge_values)
                    logger.debug(
                        f"Created {len(edge_values)} edges in the link graph.", extra=log_extra)

            num_added = len(all_inserted_links)
            logger.info(
                f"✅ Crawl complete for {url_record.url}. Added {num_added} new URLs.")
        else:
            logger.info(
                f"✅ Crawl complete for {url_record.url}. Found 0 new links.")

        url_record.status = CrawlStatus.completed
        url_record.processed_at = datetime.now(timezone.utc)
    except Exception as e:
        logger.error(f"Failed to crawl {url_record.url}: {e}", exc_info=False)
        url_record.status = CrawlStatus.failed
        url_record.error_message = str(e)

    db.commit()


def main():
    # This main loop is correct and does not need to change.
    logger.info("--- Starting BlogSpy Worker ---")
    try:
        artifact = joblib.load(MODEL_PATH)
        logger.info("Model loaded successfully.")
    except FileNotFoundError:
        logger.error(f"Model file not found at {MODEL_PATH}. Exiting.")
        sys.exit(1)
    while True:
        logger.debug("Starting new worker cycle", extra={
                     "event": "cycle_start", "performance": get_performance_metrics()})
        db = SessionLocal()
        try:
            job_processed = False
            for job_type, status_from, status_to, process_func, args in [
                ("crawling", CrawlStatus.pending_crawl,
                 CrawlStatus.crawling, process_crawl_task, []),
                ("classification", CrawlStatus.pending_classification,
                 CrawlStatus.classifying, process_classification_task, [artifact])
            ]:
                jobs = db.query(URL).filter(URL.status == status_from).with_for_update(
                    skip_locked=True).limit(BATCH_SIZE).all()
                if jobs:
                    job_processed = True
                    logger.info(f"Fetched {len(jobs)} {job_type} jobs.")
                    for job in jobs:
                        job.status = status_to
                    db.commit()
                    for job in jobs:
                        process_func(db, job, *args)
                    break
            if not job_processed:
                logger.info(
                    f"No pending jobs. Sleeping for {SLEEP_INTERVAL} seconds.")
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
