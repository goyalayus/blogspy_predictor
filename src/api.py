from fastapi import FastAPI, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from typing import List
import validators

from src.database import get_db, bulk_insert_urls
from src.utils import get_logger

app = FastAPI(title="BlogSpy Ingestion API")
logger = get_logger(__name__)

class URLPayload(BaseModel):
    urls: List[str] = Field(..., min_length=1, description="A list of one or more URLs to add to the queue.")

@app.post("/add-urls", status_code=status.HTTP_2_02_ACCEPTED)
def add_urls_to_queue(payload: URLPayload, db: Session = Depends(get_db)):
    """
    Accepts a list of URLs, validates their format, and adds valid
    ones to the processing queue with a 'pending_classification' status.
    Duplicates and invalid formats are ignored.
    """
    valid_urls = []
    invalid_count = 0

    for url_str in set(payload.urls):
        if validators.url(url_str):
            valid_urls.append(url_str)
        else:
            logger.warning(f"Skipping invalid URL format: {url_str}")
            invalid_count += 1
    
    if not valid_urls:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid URL formats were provided in the payload."
        )

    try:
        bulk_insert_urls(db, valid_urls)
        logger.info(f"Successfully queued {len(valid_urls)} new URLs for classification.")
        return {
            "message": "URLs have been accepted and queued for classification.",
            "queued_count": len(valid_urls),
            "invalid_format_skipped_count": invalid_count
        }
    except Exception as e:
        logger.error(f"Database error while queuing URLs: {e}", exc_info=True)
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="A database error occurred while trying to queue the URLs."
        )

@app.get("/health")
def health_check():
    return {"status": "ok"}
