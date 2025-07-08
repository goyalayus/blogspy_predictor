import sys
import pathlib
import joblib
import pandas as pd
import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import warnings
import scipy.sparse as sp
import numpy as np
import time
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

project_root = pathlib.Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.utils import get_logger
from src.database import SessionLocal, URL, CrawlStatus, bulk_insert_urls
from src.feature_engineering import extract_url_features, extract_structural_features, extract_content_features
from src.config import MODELS_DIR, ID_TO_LABEL, REQUEST_TIMEOUT, REQUEST_HEADERS

MODEL_PATH = MODELS_DIR / 'lgbm_final_model.joblib'
BATCH_SIZE = 5
SLEEP_INTERVAL = 10
PERSONAL_BLOG_LABEL = "PERSONAL_BLOG" 

logger = get_logger('worker')

def fetch_content(url: str) -> dict:
    response = requests.get(url, timeout=REQUEST_TIMEOUT, headers=REQUEST_HEADERS)
    response.raise_for_status()
    html_content = response.text
    
    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
    soup = BeautifulSoup(html_content, 'lxml')

    for script_or_style in soup(['script', 'style']):
        script_or_style.decompose()
    text_content = ' '.join(soup.stripped_strings)

    return {"html_content": html_content, "text_content": text_content}

def extract_links_from_html(html_content: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html_content, 'lxml')
    links = set()
    parsed_base = urlparse(base_url)

    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href'].strip()
        if not href or href.startswith('#') or href.startswith('mailto:'):
            continue
        
        full_url = urljoin(base_url, href)
        parsed_full = urlparse(full_url)

        if parsed_full.scheme in ['http', 'https'] and parsed_full.netloc == parsed_base.netloc:
            links.add(full_url)
    
    return list(links)


def process_classification_task(db, url_record: URL, artifact: dict):
    logger.info(f"[CLASSIFY] Processing {url_record.url}")
    try:
        site_data = fetch_content(url_record.url)
        
        vectorizer, model = artifact['vectorizer'], artifact['model']
        df = pd.DataFrame([{**site_data, "url": url_record.url}])
        
        txt_features = vectorizer.transform(df["text_content"].fillna(""))
        url_features = extract_url_features(df["url"]).to_numpy(dtype="float32")
        structural_features = extract_structural_features(df["html_content"]).to_numpy(dtype="float32")
        content_features = extract_content_features(df["text_content"]).to_numpy(dtype="float32")
        
        features = sp.hstack([txt_features, sp.csr_matrix(url_features), sp.csr_matrix(structural_features), sp.csr_matrix(content_features)], format="csr")
        
        probabilities = model.predict(features)
        prediction_id = (probabilities > 0.5).astype(int)[0]
        confidence = probabilities[0] if prediction_id == 1 else 1 - probabilities[0]
        label = ID_TO_LABEL[prediction_id].upper()

        url_record.classification = label
        url_record.confidence = float(confidence)
        url_record.processed_at = datetime.now(timezone.utc)
        
        if label == PERSONAL_BLOG_LABEL:
            url_record.status = CrawlStatus.pending_crawl
            url_record.content = site_data['text_content']
            logger.info(f"✅ Classified as PERSONAL_BLOG. Stored content and queued for crawling.")
        else:
            url_record.status = CrawlStatus.irrelevant
            url_record.content = None
            logger.info(f"❌ Classified as {label}. Marked as irrelevant, content not stored.")

    except Exception as e:
        logger.error(f"Failed to classify {url_record.url}: {e}", exc_info=False)
        url_record.status = CrawlStatus.failed
        url_record.error_message = str(e)
        url_record.content = None

    db.commit()

def process_crawl_task(db, url_record: URL):
    logger.info(f"[CRAWL] Processing {url_record.url}")
    try:
        site_data = fetch_content(url_record.url)
        
        new_links = extract_links_from_html(site_data['html_content'], url_record.url)
        if new_links:
            bulk_insert_urls(db, new_links)
            logger.info(f"Discovered and queued {len(new_links)} new links.")
        
        url_record.status = CrawlStatus.completed
        url_record.processed_at = datetime.now(timezone.utc)

    except Exception as e:
        logger.error(f"Failed to crawl {url_record.url}: {e}", exc_info=False)
        url_record.status = CrawlStatus.failed
        url_record.error_message = str(e)
    
    db.commit()

def main():
    logger.info("Starting BlogSpy Worker...")
    artifact = joblib.load(MODEL_PATH)
    logger.info("Model loaded successfully.")
    
    while True:
        db = SessionLocal()
        try:
            jobs = db.query(URL).filter(
                URL.status.in_([CrawlStatus.pending_classification, CrawlStatus.pending_crawl])
            ).with_for_update(skip_locked=True).limit(BATCH_SIZE).all()

            if not jobs:
                logger.info(f"No pending jobs. Sleeping for {SLEEP_INTERVAL} seconds.")
                db.close()
                time.sleep(SLEEP_INTERVAL)
                continue
            
            for job in jobs:
                if job.status == CrawlStatus.pending_classification:
                    job.status = CrawlStatus.classifying
                elif job.status == CrawlStatus.pending_crawl:
                    job.status = CrawlStatus.crawling
            db.commit()

            for job in jobs:
                if job.status == CrawlStatus.classifying:
                    process_classification_task(db, job, artifact)
                elif job.status == CrawlStatus.crawling:
                    process_crawl_task(db, job)

        except Exception as e:
            logger.error(f"An unexpected error occurred in the main loop: {e}", exc_info=True)
            db.rollback()
            time.sleep(SLEEP_INTERVAL)
        finally:
            if db.is_active:
                db.close()

if __name__ == "__main__":
    main()
