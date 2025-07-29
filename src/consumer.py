import os
import sys
import json
import time
import logging
import pathlib
import joblib
import pandas as pd
import scipy.sparse as sp
import psycopg2
from psycopg2.extras import execute_values

project_root = pathlib.Path(__file__).parent.parent
sys.path.append(str(project_root.resolve()))

from src.feature_engineering import (
    extract_url_features,
    extract_structural_features,
    extract_content_features,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("BlogSpyConsumer")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/blogspy_db")
MODEL_PATH = os.getenv("MODEL_PATH", str(project_root / "outputs/models/lgbm_final_model.joblib"))
BATCH_SIZE = 500
POLL_INTERVAL_SECONDS = 2

model_artifact = None

def predict_batch(df: pd.DataFrame) -> list[float]:
    """Runs the prediction pipeline on a DataFrame."""
    txt_feat = model_artifact["vectorizer"].transform(df["text_content"].fillna(""))
    num_feat_df = pd.concat(
        [
            extract_url_features(df["url"]),
            extract_structural_features(df["html_content"]),
            extract_content_features(df["text_content"]),
        ],
        axis=1,
    )
    num_feat = sp.csr_matrix(num_feat_df.to_numpy(dtype="float32"))
    features = sp.hstack([txt_feat, num_feat], format="csr")
    
    prediction_probs = model_artifact["model"].predict(features)
    return prediction_probs.tolist()

def main():
    """Main consumer loop."""
    global model_artifact
    logger.info("--- Starting BlogSpy Python Consumer (PostgreSQL Queue) ---")

    logger.info(f"Attempting to load model from: {MODEL_PATH}")
    model_artifact = joblib.load(MODEL_PATH)
    logger.info("✅ Model artifact loaded successfully.")

    try:
        conn = psycopg2.connect(DATABASE_URL)
        logger.info("✅ Successfully connected to PostgreSQL.")
    except psycopg2.OperationalError as e:
        logger.error(f"FATAL: Could not connect to PostgreSQL: {e}")
        sys.exit(1)

    while True:
        try:
            with conn.cursor() as cur:
                lock_query = """
                    WITH locked_jobs AS (
                        SELECT id FROM classification_queue WHERE status = 'new' ORDER BY id
                        FOR UPDATE SKIP LOCKED LIMIT %s
                    )
                    UPDATE classification_queue q SET status = 'processing', locked_at = NOW()
                    FROM locked_jobs lj WHERE q.id = lj.id
                    RETURNING q.id, q.url_id, q.payload;
                """
                cur.execute(lock_query, (BATCH_SIZE,))
                jobs = cur.fetchall()
                conn.commit()

            if not jobs:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            logger.info(f"Processing batch of {len(jobs)} jobs...")
            
            queue_ids = [job[0] for job in jobs]
            url_ids = [job[1] for job in jobs]
            payloads = [job[2] for job in jobs]
            df = pd.DataFrame(payloads)
            
            predictions = predict_batch(df)
            
            blogs_to_crawl = []
            irrelevant_jobs = []
            for url_id, pred in zip(url_ids, predictions):
                if pred > 0.5:
                    blogs_to_crawl.append((url_id,))
                else:
                    irrelevant_jobs.append((url_id,))
            
            processed_queue_job_ids = [(qid,) for qid in queue_ids]

            with conn.cursor() as cur:
                if blogs_to_crawl:
                    execute_values(cur, "UPDATE urls SET status = 'pending_crawl', processed_at = NOW() WHERE id IN %s", blogs_to_crawl)
                if irrelevant_jobs:
                    execute_values(cur, "UPDATE urls SET status = 'irrelevant', processed_at = NOW() WHERE id IN %s", irrelevant_jobs)
                if processed_queue_job_ids:
                    execute_values(cur, "DELETE FROM classification_queue WHERE id IN %s", processed_queue_job_ids)
            conn.commit()
            
            logger.info(f"✅ Batch processed. Updated {len(blogs_to_crawl)} blogs, {len(irrelevant_jobs)} irrelevant.")

        except psycopg2.OperationalError as e:
            logger.error(f"Database connection lost: {e}. Reconnecting...", exc_info=True)
            if conn:
                conn.close()
            time.sleep(5)
            conn = psycopg2.connect(DATABASE_URL)
        except Exception:
            logger.error("An unexpected error occurred in the main loop.", exc_info=True)
            if conn:
                conn.rollback()
            time.sleep(5)

if __name__ == "__main__":
    main()
