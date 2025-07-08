from sqlalchemy import create_engine, Column, Integer, String, Text, Float, DateTime, Enum
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql import func
import enum
from src.settings import settings

engine = create_engine(settings.DATABASE_URL)
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

class URL(Base):
    __tablename__ = 'urls'

    id = Column(Integer, primary_key=True, index=True)
    url = Column(Text, unique=True, nullable=False)
    status = Column(Enum(CrawlStatus, name="crawl_status"), nullable=False, default=CrawlStatus.pending_classification)
    content = Column(Text)
    classification = Column(Text)
    confidence = Column(Float)
    error_message = Column(Text)
    processed_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def bulk_insert_urls(db_session, url_list: list[str]):
    """
    Inserts multiple URLs into the database, ignoring any that already exist.
    Expects a list of URL strings.
    """
    if not url_list:
        return

    insert_values = [{"url": url} for url in url_list]

    stmt = pg_insert(URL).values(insert_values)
    stmt = stmt.on_conflict_do_nothing(index_elements=['url'])
    db_session.execute(stmt)
    db_session.commit()
