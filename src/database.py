# src/database.py

from sqlalchemy import create_engine, Column, Integer, String, Text, Float, DateTime, Enum, PrimaryKeyConstraint
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql import func
import enum
from urllib.parse import urlparse

from src.settings import settings

# Increase the connection pool size to support high concurrency from worker threads.
engine = create_engine(
    settings.DATABASE_URL,
    pool_size=100,
    max_overflow=20
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class CrawlStatus(str, enum.Enum):
    pending_classification = 'pending_classification'
    classifying = 'classifying'
    pending_crawl = 'pending_crawl'
    crawling = 'crawling'
    completed = 'completed'
    irrelevant = 'irrelevant'
    failed = 'failed'


class RenderingType(str, enum.Enum):
    SSR = 'SSR'
    CSR = 'CSR'
    UNKNOWN = 'UNKNOWN'

# --- URL TABLE ---
# This model is now back to its correct state without the extra columns.


class URL(Base):
    __tablename__ = 'urls'
    id = Column(Integer, primary_key=True, index=True)
    url = Column(Text, unique=True, nullable=False)
    netloc = Column(Text, nullable=False, index=True)
    status = Column(Enum(CrawlStatus, name="crawl_status"),
                    nullable=False, default=CrawlStatus.pending_classification)
    rendering = Column(Enum(RenderingType, name="rendering_type"),
                       nullable=False, default=RenderingType.UNKNOWN)
    content = Column(Text)
    title = Column(Text)
    description = Column(Text)
    error_message = Column(Text)
    processed_at = Column(DateTime(timezone=True))
    # <-- ADDED FOR FAULT TOLERANCE
    locked_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True),
                        nullable=False, server_default=func.now())


class URLEdge(Base):
    __tablename__ = 'url_edges'
    source_url_id = Column(Integer, nullable=False)
    dest_url_id = Column(Integer, nullable=False)
    __table_args__ = (PrimaryKeyConstraint('source_url_id', 'dest_url_id'),)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def bulk_insert_urls(db_session: Session, url_list: list[str]):
    """Inserts multiple URLs with default 'pending_classification' status."""
    if not url_list:
        return
    insert_values = [{"url": url, "netloc": urlparse(
        url).netloc} for url in url_list if urlparse(url).netloc]
    if not insert_values:
        return
    stmt = pg_insert(URL).values(
        insert_values).on_conflict_do_nothing(index_elements=['url'])
    db_session.execute(stmt)
    # Commit is handled by the calling task
