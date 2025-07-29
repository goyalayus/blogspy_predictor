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
from urllib.parse import urlparse

project_root = pathlib.Path(__file__).parent.parent
sys.path.append(str(project_root.resolve()))

try:
    from src.feature_engineering import (
        extract_url_features,
        extract_structural_features,
        extract_content_features,
    )
except ImportError as e:
    print(f"FATAL: Could not import feature engineering modules: {e}")
    print(
        f"Make sure the project structure is correct and modules exist in: {project_root / 'src'}"
    )
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("BlogSpyConsumer")

BATCH_SIZE = 500
POLL_INTERVAL_SECONDS = 2

model_artifact = None


def validate_environment():
    """Validate all required environment variables and configurations."""
    logger.info("🔍 Validating environment and configuration...")

    issues = []

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        issues.append("❌ DATABASE_URL environment variable is not set")
        logger.error("DATABASE_URL environment variable is not set!")
        logger.info(
            "Please set it like: export DATABASE_URL='postgresql://user:password@localhost:5432/dbname'"
        )
    else:
        logger.info(f"✅ DATABASE_URL found: {database_url[:20]}...")
        try:
            parsed = urlparse(database_url)
            if not all(
                [
                    parsed.scheme,
                    parsed.hostname,
                    parsed.username,
                    parsed.password,
                    parsed.path,
                ]
            ):
                issues.append(
                    "❌ DATABASE_URL format appears invalid (missing components)"
                )
        except Exception as e:
            issues.append(f"❌ DATABASE_URL format validation failed: {e}")

    model_path = os.getenv("MODEL_PATH")
    if not model_path:
        model_path = str(project_root / "outputs/models/lgbm_final_model.joblib")
        logger.warning(
            f"⚠️  MODEL_PATH environment variable not set, using default: {model_path}"
        )
    else:
        logger.info(f"✅ MODEL_PATH found: {model_path}")

    model_file = pathlib.Path(model_path)
    if not model_file.exists():
        issues.append(f"❌ Model file does not exist: {model_path}")
        logger.error(f"Model file not found at: {model_path}")
        if not model_file.parent.exists():
            logger.error(f"Model directory also doesn't exist: {model_file.parent}")
    elif not model_file.is_file():
        issues.append(f"❌ Model path exists but is not a file: {model_path}")
    else:
        logger.info(f"✅ Model file exists: {model_path}")

    src_dir = project_root / "src"
    if not src_dir.exists():
        issues.append(f"❌ Source directory does not exist: {src_dir}")
    else:
        logger.info(f"✅ Source directory found: {src_dir}")

    feature_eng_file = src_dir / "feature_engineering.py"
    if not feature_eng_file.exists():
        issues.append(f"❌ Feature engineering module not found: {feature_eng_file}")
    else:
        logger.info("✅ Feature engineering module found")

    if issues:
        logger.error("🚨 CONFIGURATION VALIDATION FAILED:")
        for issue in issues:
            logger.error(f"   {issue}")
        logger.error("Please fix the above issues before running the script.")
        return False, None

    logger.info("✅ All environment validation checks passed!")
    return True, model_path


def validate_database_connection(database_url):
    """Test database connection and validate schema."""
    logger.info("🔍 Validating database connection...")

    try:
        conn = psycopg2.connect(database_url)
        logger.info("✅ Database connection successful")

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'classification_queue'
                );
            """
            )
            if not cur.fetchone()[0]:
                logger.error("❌ Required table 'classification_queue' does not exist")
                conn.close()
                return False, None

            cur.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'urls'
                );
            """
            )
            if not cur.fetchone()[0]:
                logger.error("❌ Required table 'urls' does not exist")
                conn.close()
                return False, None

            cur.execute(
                """
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'classification_queue';
            """
            )
            columns = [row[0] for row in cur.fetchall()]
            required_columns = ["id", "url_id", "payload", "status", "locked_at"]
            missing_columns = [col for col in required_columns if col not in columns]
            if missing_columns:
                logger.error(
                    f"❌ Missing columns in classification_queue: {missing_columns}"
                )
                conn.close()
                return False, None

        logger.info("✅ Database schema validation passed")
        return True, conn

    except psycopg2.OperationalError as e:
        logger.error(f"❌ Database connection failed: {e}")
        logger.error("Please check your DATABASE_URL and ensure PostgreSQL is running")
        return False, None
    except Exception as e:
        logger.error(f"❌ Database validation failed: {e}")
        return False, None


def load_and_validate_model(model_path):
    """Load and validate the model artifact."""
    logger.info(f"🔍 Loading model from: {model_path}")

    try:
        model_artifact = joblib.load(model_path)
        logger.info("✅ Model file loaded successfully")

        if not isinstance(model_artifact, dict):
            logger.error("❌ Model artifact is not a dictionary")
            return False, None

        required_keys = ["model", "vectorizer"]
        missing_keys = [key for key in required_keys if key not in model_artifact]
        if missing_keys:
            logger.error(f"❌ Model artifact missing required keys: {missing_keys}")
            logger.error(f"Available keys: {list(model_artifact.keys())}")
            return False, None

        if not hasattr(model_artifact["model"], "predict"):
            logger.error("❌ Model object doesn't have 'predict' method")
            return False, None

        if not hasattr(model_artifact["vectorizer"], "transform"):
            logger.error("❌ Vectorizer object doesn't have 'transform' method")
            return False, None

        logger.info("✅ Model artifact validation passed")
        return True, model_artifact

    except FileNotFoundError:
        logger.error(f"❌ Model file not found: {model_path}")
        return False, None
    except Exception as e:
        logger.error(f"❌ Failed to load model: {e}")
        return False, None


def predict_batch(df: pd.DataFrame) -> list[float]:
    """Runs the prediction pipeline on a DataFrame."""
    try:
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
    except Exception as e:
        logger.error(f"❌ Prediction failed: {e}")
        raise


def main():
    """Main consumer loop with comprehensive validation."""
    global model_artifact

    logger.info("🚀 Starting BlogSpy Python Consumer (PostgreSQL Queue)")
    logger.info("=" * 60)

    env_valid, model_path = validate_environment()
    if not env_valid:
        logger.error("🚨 Environment validation failed. Exiting.")
        sys.exit(1)

    database_url = os.getenv(
        "DATABASE_URL", "postgresql://user:password@localhost:5432/blogspy_db"
    )
    db_valid, conn = validate_database_connection(database_url)
    if not db_valid:
        logger.error("🚨 Database validation failed. Exiting.")
        sys.exit(1)

    model_valid, model_artifact = load_and_validate_model(model_path)
    if not model_valid:
        logger.error("🚨 Model validation failed. Exiting.")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("✅ ALL VALIDATIONS PASSED - Starting main processing loop")
    logger.info("=" * 60)

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

            logger.info(f"📦 Processing batch of {len(jobs)} jobs...")

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
                    execute_values(
                        cur,
                        "UPDATE urls SET status = 'pending_crawl', processed_at = NOW() WHERE id IN %s",
                        blogs_to_crawl,
                    )
                if irrelevant_jobs:
                    execute_values(
                        cur,
                        "UPDATE urls SET status = 'irrelevant', processed_at = NOW() WHERE id IN %s",
                        irrelevant_jobs,
                    )
                if processed_queue_job_ids:
                    execute_values(
                        cur,
                        "DELETE FROM classification_queue WHERE id IN %s",
                        processed_queue_job_ids,
                    )
            conn.commit()

            logger.info(
                f"✅ Batch processed. Updated {len(blogs_to_crawl)} blogs, {len(irrelevant_jobs)} irrelevant."
            )

        except psycopg2.OperationalError as e:
            logger.error(
                f"💥 Database connection lost: {e}. Reconnecting...", exc_info=True
            )
            if conn:
                conn.close()
            time.sleep(5)
            try:
                conn = psycopg2.connect(database_url)
                logger.info("✅ Database reconnected successfully")
            except Exception as reconn_e:
                logger.error(f"❌ Reconnection failed: {reconn_e}")
        except Exception as e:
            logger.error(
                f"💥 An unexpected error occurred in the main loop: {e}", exc_info=True
            )
            if conn:
                conn.rollback()
            time.sleep(5)


if __name__ == "__main__":
    main()
