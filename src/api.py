from fastapi import FastAPI, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from typing import List, Dict, Any
import validators

from src.database import get_db, bulk_insert_urls, URL, CrawlStatus
from src.utils import get_logger

app = FastAPI(title="BlogSpy Ingestion API & Cache")
logger = get_logger(__name__)

class URLPayload(BaseModel):
    urls: List[str] = Field(..., min_length=1, description="A list of one or more URLs to process.")

@app.post("/add-urls", status_code=status.HTTP_200_OK)
def add_urls_to_queue(payload: URLPayload, db: Session = Depends(get_db)):
    FINAL_STATUSES = {CrawlStatus.completed, CrawlStatus.irrelevant, CrawlStatus.failed}

    response_data: List[Dict[str, Any]] = []
    urls_to_queue = []

    unique_urls = set(payload.urls)

    for url_str in unique_urls:
        if not validators.url(url_str):
            response_data.append({"url": url_str, "status": "error", "detail": "Invalid URL format."})
            continue

        existing_record = db.query(URL).filter(URL.url == url_str).first()

        if existing_record:
            if existing_record.status in FINAL_STATUSES:
                response_data.append({
                    "url": url_str,
                    "status": "cached",
                    "result": {
                        "classification": existing_record.classification,
                        "confidence": existing_record.confidence,
                        "processed_at": existing_record.processed_at,
                        "db_status": existing_record.status.value
                    }
                })
            else:
                response_data.append({
                    "url": url_str,
                    "status": "already_queued",
                    "detail": f"URL is currently in state: {existing_record.status.value}"
                })
        else:
            urls_to_queue.append(url_str)
            response_data.append({"url": url_str, "status": "newly_queued"})

    if urls_to_queue:
        try:
            bulk_insert_urls(db, urls_to_queue)
            logger.info(f"Successfully queued {len(urls_to_queue)} new URLs for classification.")
        except Exception as e:
            logger.error(f"Database error while queuing URLs: {e}", exc_info=True)
            db.rollback()
            for item in response_data:
                if item["url"] in urls_to_queue:
                    item["status"] = "error"
                    item["detail"] = "Failed to queue due to a database error."

    return {"results": response_data}

@app.get("/health")
def health_check():
    return {"status": "ok"}
