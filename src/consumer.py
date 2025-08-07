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
import traceback
import psutil
import gc
from datetime import datetime

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
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("BlogSpyConsumer")

BATCH_SIZE = 500
POLL_INTERVAL_SECONDS = 2

model_artifact = None


def log_memory_usage(operation_name):
    """Log current memory usage for debugging."""
    try:
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        memory_mb = memory_info.rss / 1024 / 1024
        logger.debug(
            f"🧠 MEMORY [{operation_name}]: {memory_mb:.2f} MB RSS, {memory_info.vms / 1024 / 1024:.2f} MB VMS"
        )
    except Exception as e:
        logger.warning(f"⚠️ Could not get memory usage for {operation_name}: {e}")


def log_timing(func):
    """Decorator to log function execution time."""

    def wrapper(*args, **kwargs):
        start_time = time.time()
        start_timestamp = datetime.now().isoformat()
        logger.debug(f"⏱️ TIMING START [{func.__name__}]: {start_timestamp}")

        try:
            result = func(*args, **kwargs)
            end_time = time.time()
            duration = end_time - start_time
            end_timestamp = datetime.now().isoformat()
            logger.debug(
                f"⏱️ TIMING END [{func.__name__}]: {end_timestamp} | Duration: {duration:.4f}s"
            )
            return result
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            end_timestamp = datetime.now().isoformat()
            logger.error(
                f"⏱️ TIMING ERROR [{func.__name__}]: {end_timestamp} | Duration: {duration:.4f}s | Error: {e}"
            )
            raise

    return wrapper


@log_timing
def validate_environment():
    """Validate all required environment variables and configurations."""
    logger.info(
        "🔍 VALIDATION START: Environment and configuration validation beginning..."
    )
    logger.debug(f"🔍 Current working directory: {os.getcwd()}")
    logger.debug(f"🔍 Script file location: {__file__}")
    logger.debug(f"🔍 Project root resolved to: {project_root.resolve()}")
    logger.debug(f"🔍 Python path: {sys.path}")

    log_memory_usage("environment_validation_start")

    issues = []
    validation_start = time.time()

    logger.debug("🔍 Checking DATABASE_URL environment variable...")
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        issues.append("❌ DATABASE_URL environment variable is not set")
        logger.error("DATABASE_URL environment variable is not set!")
        logger.info(
            "Please set it like: export DATABASE_URL='postgresql://user:password@localhost:5432/dbname'"
        )
        logger.debug("🔍 Available environment variables:")
        for key in sorted(os.environ.keys()):
            if "DATABASE" in key.upper() or "DB" in key.upper():
                logger.debug(
                    f"   🔍 {key}={os.environ[key][:50]}..."
                    if len(os.environ[key]) > 50
                    else f"   🔍 {key}={os.environ[key]}"
                )
    else:
        logger.info(f"✅ DATABASE_URL found: {database_url[:20]}...")
        logger.debug(f"🔍 Full DATABASE_URL length: {len(database_url)} characters")

        try:
            logger.debug("🔍 Parsing DATABASE_URL...")
            parsed = urlparse(database_url)
            logger.debug(f"🔍 Parsed URL components:")
            logger.debug(f"   🔍 Scheme: {parsed.scheme}")
            logger.debug(f"   🔍 Hostname: {parsed.hostname}")
            logger.debug(f"   🔍 Port: {parsed.port}")
            logger.debug(f"   🔍 Username: {parsed.username}")
            logger.debug(f"   🔍 Password: {'***' if parsed.password else 'None'}")
            logger.debug(f"   🔍 Database: {parsed.path}")

            if not all(
                [
                    parsed.scheme,
                    parsed.hostname,
                    parsed.username,
                    parsed.password,
                    parsed.path,
                ]
            ):
                missing_components = []
                if not parsed.scheme:
                    missing_components.append("scheme")
                if not parsed.hostname:
                    missing_components.append("hostname")
                if not parsed.username:
                    missing_components.append("username")
                if not parsed.password:
                    missing_components.append("password")
                if not parsed.path:
                    missing_components.append("database_path")

                issues.append(
                    f"❌ DATABASE_URL format appears invalid (missing components: {', '.join(missing_components)})"
                )
                logger.error(f"🔍 Missing URL components: {missing_components}")
        except Exception as e:
            issues.append(f"❌ DATABASE_URL format validation failed: {e}")
            logger.error(f"🔍 URL parsing exception: {e}")
            logger.debug(f"🔍 URL parsing traceback: {traceback.format_exc()}")

    logger.debug("🔍 Checking MODEL_PATH environment variable...")
    model_path = os.getenv("MODEL_PATH")
    if not model_path:
        model_path = str(project_root / "outputs/models/lgbm_final_model.joblib")
        logger.warning(
            f"⚠️  MODEL_PATH environment variable not set, using default: {model_path}"
        )
        logger.debug(
            f"🔍 Default model path constructed from project_root: {project_root}"
        )
    else:
        logger.info(f"✅ MODEL_PATH found: {model_path}")
        logger.debug(f"🔍 MODEL_PATH length: {len(model_path)} characters")

    logger.debug(f"🔍 Checking model file existence at: {model_path}")
    model_file = pathlib.Path(model_path)
    logger.debug(f"🔍 Model file path object: {model_file}")
    logger.debug(f"🔍 Model file absolute path: {model_file.resolve()}")

    if not model_file.exists():
        issues.append(f"❌ Model file does not exist: {model_path}")
        logger.error(f"Model file not found at: {model_path}")
        logger.debug(f"🔍 Checking if parent directory exists: {model_file.parent}")
        logger.debug(f"🔍 Parent directory absolute path: {model_file.parent.resolve()}")

        if not model_file.parent.exists():
            logger.error(f"Model directory also doesn't exist: {model_file.parent}")
            logger.debug(f"🔍 Listing contents of grandparent directory if it exists:")
            grandparent = model_file.parent.parent
            if grandparent.exists():
                try:
                    contents = list(grandparent.iterdir())
                    logger.debug(
                        f"🔍 Grandparent directory contents: {[str(p) for p in contents]}"
                    )
                except Exception as e:
                    logger.debug(f"🔍 Could not list grandparent directory: {e}")
        else:
            logger.debug(f"🔍 Parent directory exists, listing contents:")
            try:
                contents = list(model_file.parent.iterdir())
                logger.debug(
                    f"🔍 Parent directory contents: {[str(p) for p in contents]}"
                )
            except Exception as e:
                logger.debug(f"🔍 Could not list parent directory: {e}")
    elif not model_file.is_file():
        issues.append(f"❌ Model path exists but is not a file: {model_path}")
        logger.error(f"🔍 Model path exists but is not a file. Type: {type(model_file)}")
        logger.debug(f"🔍 Is directory: {model_file.is_dir()}")
        logger.debug(f"🔍 Is symlink: {model_file.is_symlink()}")
    else:
        logger.info(f"✅ Model file exists: {model_path}")
        try:
            file_stats = model_file.stat()
            logger.debug(
                f"🔍 Model file size: {file_stats.st_size} bytes ({file_stats.st_size / 1024 / 1024:.2f} MB)"
            )
            logger.debug(
                f"🔍 Model file modified time: {datetime.fromtimestamp(file_stats.st_mtime)}"
            )
            logger.debug(f"🔍 Model file permissions: {oct(file_stats.st_mode)}")
        except Exception as e:
            logger.warning(f"⚠️ Could not get model file stats: {e}")

    logger.debug(f"🔍 Checking source directory: {project_root / 'src'}")
    src_dir = project_root / "src"
    logger.debug(f"🔍 Source directory absolute path: {src_dir.resolve()}")

    if not src_dir.exists():
        issues.append(f"❌ Source directory does not exist: {src_dir}")
        logger.error(f"🔍 Source directory missing. Checking project root contents:")
        try:
            project_contents = list(project_root.iterdir())
            logger.debug(
                f"🔍 Project root contents: {[str(p) for p in project_contents]}"
            )
        except Exception as e:
            logger.debug(f"🔍 Could not list project root: {e}")
    else:
        logger.info(f"✅ Source directory found: {src_dir}")
        logger.debug(f"🔍 Listing source directory contents:")
        try:
            src_contents = list(src_dir.iterdir())
            logger.debug(
                f"🔍 Source directory contents: {[str(p) for p in src_contents]}"
            )
        except Exception as e:
            logger.debug(f"🔍 Could not list source directory: {e}")

    feature_eng_file = src_dir / "feature_engineering.py"
    logger.debug(f"🔍 Checking feature engineering module: {feature_eng_file}")
    logger.debug(
        f"🔍 Feature engineering module absolute path: {feature_eng_file.resolve()}"
    )

    if not feature_eng_file.exists():
        issues.append(f"❌ Feature engineering module not found: {feature_eng_file}")
        logger.error(f"🔍 Feature engineering module missing")
    else:
        logger.info("✅ Feature engineering module found")
        try:
            fe_stats = feature_eng_file.stat()
            logger.debug(f"🔍 Feature engineering file size: {fe_stats.st_size} bytes")
            logger.debug(
                f"🔍 Feature engineering file modified: {datetime.fromtimestamp(fe_stats.st_mtime)}"
            )
        except Exception as e:
            logger.warning(f"⚠️ Could not get feature engineering file stats: {e}")

    validation_duration = time.time() - validation_start
    logger.debug(
        f"🔍 Environment validation completed in {validation_duration:.4f} seconds"
    )
    log_memory_usage("environment_validation_end")

    if issues:
        logger.error("🚨 CONFIGURATION VALIDATION FAILED:")
        logger.error(f"🚨 Total issues found: {len(issues)}")
        for i, issue in enumerate(issues, 1):
            logger.error(f"   🚨 Issue {i}: {issue}")
        logger.error("Please fix the above issues before running the script.")
        logger.debug(f"🔍 Validation failed after {validation_duration:.4f} seconds")
        return False, None

    logger.info("✅ All environment validation checks passed!")
    logger.debug(f"🔍 Validation succeeded after {validation_duration:.4f} seconds")
    return True, model_path


@log_timing
def validate_database_connection(database_url):
    """Test database connection and validate schema."""
    logger.info("🔍 VALIDATION START: Database connection validation beginning...")
    logger.debug(f"🔍 Attempting to connect to database URL: {database_url[:30]}...")

    log_memory_usage("db_validation_start")
    connection_start = time.time()

    try:
        logger.debug("🔍 Creating PostgreSQL connection...")
        conn = psycopg2.connect(database_url)
        connection_time = time.time() - connection_start
        logger.info(f"✅ Database connection successful (took {connection_time:.4f}s)")

        logger.debug(f"🔍 Connection status: {conn.status}")
        logger.debug(f"🔍 Connection info: {conn.get_dsn_parameters()}")
        logger.debug(f"🔍 Server version: {conn.server_version}")
        logger.debug(f"🔍 Protocol version: {conn.protocol_version}")

        logger.debug("🔍 Starting database schema validation...")
        schema_start = time.time()

        with conn.cursor() as cur:
            logger.debug("🔍 Cursor created successfully")

            logger.debug("🔍 Checking for 'classification_queue' table...")
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'classification_queue'
                );
            """
            )
            queue_table_exists = cur.fetchone()[0]
            logger.debug(f"🔍 classification_queue table exists: {queue_table_exists}")

            if not queue_table_exists:
                logger.error("❌ Required table 'classification_queue' does not exist")
                logger.debug("🔍 Listing all available tables...")
                cur.execute(
                    """
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public';
                """
                )
                tables = [row[0] for row in cur.fetchall()]
                logger.debug(f"🔍 Available tables: {tables}")
                conn.close()
                return False, None

            logger.debug("🔍 Checking for 'urls' table...")
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'urls'
                );
            """
            )
            urls_table_exists = cur.fetchone()[0]
            logger.debug(f"🔍 urls table exists: {urls_table_exists}")

            if not urls_table_exists:
                logger.error("❌ Required table 'urls' does not exist")
                logger.debug("🔍 Listing all available tables...")
                cur.execute(
                    """
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public';
                """
                )
                tables = [row[0] for row in cur.fetchall()]
                logger.debug(f"🔍 Available tables: {tables}")
                conn.close()
                return False, None

            logger.debug("🔍 Validating classification_queue table columns...")
            cur.execute(
                """
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_name = 'classification_queue'
                ORDER BY ordinal_position;
            """
            )
            column_info = cur.fetchall()
            columns = [row[0] for row in column_info]
            logger.debug(f"🔍 classification_queue columns found: {columns}")

            for col_name, data_type, is_nullable in column_info:
                logger.debug(
                    f"🔍   Column: {col_name}, Type: {data_type}, Nullable: {is_nullable}"
                )

            required_columns = ["id", "url_id", "payload", "status", "locked_at"]
            missing_columns = [col for col in required_columns if col not in columns]
            logger.debug(f"🔍 Required columns: {required_columns}")
            logger.debug(f"🔍 Missing columns: {missing_columns}")

            if missing_columns:
                logger.error(
                    f"❌ Missing columns in classification_queue: {missing_columns}"
                )
                conn.close()
                return False, None

            logger.debug("🔍 Validating urls table columns...")
            cur.execute(
                """
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_name = 'urls'
                ORDER BY ordinal_position;
            """
            )
            urls_column_info = cur.fetchall()
            urls_columns = [row[0] for row in urls_column_info]
            logger.debug(f"🔍 urls table columns found: {urls_columns}")

            for col_name, data_type, is_nullable in urls_column_info:
                logger.debug(
                    f"🔍   Column: {col_name}, Type: {data_type}, Nullable: {is_nullable}"
                )

            logger.debug("🔍 Checking table row counts...")
            cur.execute("SELECT COUNT(*) FROM classification_queue;")
            queue_count = cur.fetchone()[0]
            logger.debug(f"🔍 classification_queue row count: {queue_count}")

            cur.execute("SELECT COUNT(*) FROM urls;")
            urls_count = cur.fetchone()[0]
            logger.debug(f"🔍 urls table row count: {urls_count}")

            logger.debug("🔍 Checking classification_queue status distribution...")
            cur.execute(
                "SELECT status, COUNT(*) FROM classification_queue GROUP BY status;"
            )
            status_dist = cur.fetchall()
            logger.debug(f"🔍 Queue status distribution: {dict(status_dist)}")

        schema_time = time.time() - schema_start
        total_time = time.time() - connection_start
        logger.debug(f"🔍 Schema validation completed in {schema_time:.4f}s")
        logger.info(
            f"✅ Database schema validation passed (total time: {total_time:.4f}s)"
        )

        log_memory_usage("db_validation_end")
        return True, conn

    except psycopg2.OperationalError as e:
        connection_time = time.time() - connection_start
        logger.error(f"❌ Database connection failed after {connection_time:.4f}s: {e}")
        logger.error("Please check your DATABASE_URL and ensure PostgreSQL is running")
        logger.debug(f"🔍 Connection error traceback: {traceback.format_exc()}")
        return False, None
    except Exception as e:
        total_time = time.time() - connection_start
        logger.error(f"❌ Database validation failed after {total_time:.4f}s: {e}")
        logger.debug(f"🔍 Validation error traceback: {traceback.format_exc()}")
        return False, None


@log_timing
def load_and_validate_model(model_path):
    """Load and validate the model artifact."""
    logger.info(f"🔍 VALIDATION START: Loading model from: {model_path}")

    log_memory_usage("model_load_start")
    load_start = time.time()

    try:
        logger.debug("🔍 Starting joblib.load()...")
        load_file_start = time.time()
        model_artifact = joblib.load(model_path)
        load_file_time = time.time() - load_file_start
        logger.info(f"✅ Model file loaded successfully (took {load_file_time:.4f}s)")

        log_memory_usage("model_loaded")

        logger.debug(f"🔍 Model artifact type: {type(model_artifact)}")
        logger.debug(
            f"🔍 Model artifact memory size: {sys.getsizeof(model_artifact)} bytes"
        )

        if not isinstance(model_artifact, dict):
            logger.error(
                f"❌ Model artifact is not a dictionary, got: {type(model_artifact)}"
            )
            return False, None

        logger.debug(f"🔍 Model artifact keys: {list(model_artifact.keys())}")

        required_keys = ["model", "vectorizer"]
        missing_keys = [key for key in required_keys if key not in model_artifact]
        logger.debug(f"🔍 Required keys: {required_keys}")
        logger.debug(f"🔍 Missing keys: {missing_keys}")

        if missing_keys:
            logger.error(f"❌ Model artifact missing required keys: {missing_keys}")
            logger.error(f"Available keys: {list(model_artifact.keys())}")
            return False, None

        model_obj = model_artifact["model"]
        logger.debug(f"🔍 Model object type: {type(model_obj)}")
        logger.debug(f"🔍 Model object memory size: {sys.getsizeof(model_obj)} bytes")
        logger.debug(
            f"🔍 Model object attributes: {[attr for attr in dir(model_obj) if not attr.startswith('_')]}"
        )

        if not hasattr(model_obj, "predict"):
            logger.error("❌ Model object doesn't have 'predict' method")
            logger.debug(
                f"🔍 Available methods: {[method for method in dir(model_obj) if callable(getattr(model_obj, method))]}"
            )
            return False, None

        vectorizer_obj = model_artifact["vectorizer"]
        logger.debug(f"🔍 Vectorizer object type: {type(vectorizer_obj)}")
        logger.debug(
            f"🔍 Vectorizer object memory size: {sys.getsizeof(vectorizer_obj)} bytes"
        )
        logger.debug(
            f"🔍 Vectorizer object attributes: {[attr for attr in dir(vectorizer_obj) if not attr.startswith('_')]}"
        )

        if not hasattr(vectorizer_obj, "transform"):
            logger.error("❌ Vectorizer object doesn't have 'transform' method")
            logger.debug(
                f"🔍 Available methods: {[method for method in dir(vectorizer_obj) if callable(getattr(vectorizer_obj, method))]}"
            )
            return False, None

        try:
            if hasattr(model_obj, "n_features_"):
                logger.debug(f"🔍 Model expects {model_obj.n_features_} features")
            if hasattr(model_obj, "classes_"):
                logger.debug(f"🔍 Model classes: {model_obj.classes_}")
            if hasattr(vectorizer_obj, "vocabulary_"):
                vocab_size = (
                    len(vectorizer_obj.vocabulary_) if vectorizer_obj.vocabulary_ else 0
                )
                logger.debug(f"🔍 Vectorizer vocabulary size: {vocab_size}")
        except Exception as e:
            logger.warning(f"⚠️ Could not get additional model info: {e}")

        load_time = time.time() - load_start
        logger.info(
            f"✅ Model artifact validation passed (total time: {load_time:.4f}s)"
        )
        log_memory_usage("model_validation_end")

        return True, model_artifact

    except FileNotFoundError:
        load_time = time.time() - load_start
        logger.error(f"❌ Model file not found after {load_time:.4f}s: {model_path}")
        return False, None
    except Exception as e:
        load_time = time.time() - load_start
        logger.error(f"❌ Failed to load model after {load_time:.4f}s: {e}")
        logger.debug(f"🔍 Model loading error traceback: {traceback.format_exc()}")
        return False, None


@log_timing
def predict_batch(df: pd.DataFrame) -> list[float]:
    """Runs the prediction pipeline on a DataFrame."""
    logger.debug(f"🔮 PREDICTION START: Processing batch of {len(df)} items")
    logger.debug(f"🔮 DataFrame shape: {df.shape}")
    logger.debug(f"🔮 DataFrame columns: {list(df.columns)}")
    logger.debug(f"🔮 DataFrame memory usage: {df.memory_usage(deep=True).sum()} bytes")

    log_memory_usage("prediction_start")
    prediction_start = time.time()

    try:
        logger.debug("🔮 Starting text feature extraction...")
        text_start = time.time()

        text_content = df["text_content"].fillna("")
        logger.debug(f"🔮 Text content stats:")
        logger.debug(
            f"   🔮 Non-null text entries: {(df['text_content'].notna()).sum()}"
        )
        logger.debug(f"   🔮 Null text entries: {(df['text_content'].isna()).sum()}")
        logger.debug(f"   🔮 Empty text entries: {(text_content == '').sum()}")

        if len(text_content) > 0:
            text_lengths = text_content.str.len()
            logger.debug(
                f"   🔮 Text length stats: min={text_lengths.min()}, max={text_lengths.max()}, mean={text_lengths.mean():.2f}"
            )

        txt_feat = model_artifact["vectorizer"].transform(text_content)
        text_time = time.time() - text_start

        logger.debug(f"🔮 Text features extracted in {text_time:.4f}s")
        logger.debug(f"🔮 Text feature matrix shape: {txt_feat.shape}")
        logger.debug(f"🔮 Text feature matrix type: {type(txt_feat)}")
        logger.debug(
            f"🔮 Text feature matrix density: {txt_feat.nnz / (txt_feat.shape[0] * txt_feat.shape[1]):.4f}"
        )

        log_memory_usage("text_features_extracted")

        logger.debug("🔮 Starting URL feature extraction...")
        url_start = time.time()

        url_feat = extract_url_features(df["url"])
        url_time = time.time() - url_start

        logger.debug(f"🔮 URL features extracted in {url_time:.4f}s")
        logger.debug(f"🔮 URL features shape: {url_feat.shape}")
        logger.debug(f"🔮 URL features columns: {list(url_feat.columns)}")
        logger.debug(
            f"🔮 URL features memory: {url_feat.memory_usage(deep=True).sum()} bytes"
        )

        log_memory_usage("url_features_extracted")

        logger.debug("🔮 Starting structural feature extraction...")
        struct_start = time.time()

        html_content = df["html_content"]
        logger.debug(f"🔮 HTML content stats:")
        logger.debug(f"   🔮 Non-null HTML entries: {(html_content.notna()).sum()}")
        logger.debug(f"   🔮 Null HTML entries: {(html_content.isna()).sum()}")

        if len(html_content) > 0 and html_content.notna().any():
            html_lengths = html_content.fillna("").str.len()
            logger.debug(
                f"   🔮 HTML length stats: min={html_lengths.min()}, max={html_lengths.max()}, mean={html_lengths.mean():.2f}"
            )

        struct_feat = extract_structural_features(html_content)
        struct_time = time.time() - struct_start

        logger.debug(f"🔮 Structural features extracted in {struct_time:.4f}s")
        logger.debug(f"🔮 Structural features shape: {struct_feat.shape}")
        logger.debug(f"🔮 Structural features columns: {list(struct_feat.columns)}")
        logger.debug(
            f"🔮 Structural features memory: {struct_feat.memory_usage(deep=True).sum()} bytes"
        )

        log_memory_usage("structural_features_extracted")

        logger.debug("🔮 Starting content feature extraction...")
        content_start = time.time()

        content_feat = extract_content_features(df["text_content"])
        content_time = time.time() - content_start

        logger.debug(f"🔮 Content features extracted in {content_time:.4f}s")
        logger.debug(f"🔮 Content features shape: {content_feat.shape}")
        logger.debug(f"🔮 Content features columns: {list(content_feat.columns)}")
        logger.debug(
            f"🔮 Content features memory: {content_feat.memory_usage(deep=True).sum()} bytes"
        )

        log_memory_usage("content_features_extracted")

        logger.debug("🔮 Concatenating numerical features...")
        concat_start = time.time()

        num_feat_df = pd.concat([url_feat, struct_feat, content_feat], axis=1)
        concat_time = time.time() - concat_start

        logger.debug(f"🔮 Numerical features concatenated in {concat_time:.4f}s")
        logger.debug(f"🔮 Combined numerical features shape: {num_feat_df.shape}")
        logger.debug(
            f"🔮 Combined numerical features memory: {num_feat_df.memory_usage(deep=True).sum()} bytes"
        )

        nan_counts = num_feat_df.isna().sum()
        if nan_counts.any():
            logger.warning(f"⚠️ Found NaN values in numerical features:")
            for col, count in nan_counts[nan_counts > 0].items():
                logger.warning(f"   ⚠️ {col}: {count} NaN values")

        log_memory_usage("numerical_features_concatenated")

        logger.debug("🔮 Converting numerical features to sparse matrix...")
        sparse_start = time.time()

        num_feat = sp.csr_matrix(num_feat_df.to_numpy(dtype="float32"))
        sparse_time = time.time() - sparse_start

        logger.debug(f"🔮 Numerical features converted to sparse in {sparse_time:.4f}s")
        logger.debug(f"🔮 Numerical sparse matrix shape: {num_feat.shape}")
        logger.debug(
            f"🔮 Numerical sparse matrix density: {num_feat.nnz / (num_feat.shape[0] * num_feat.shape[1]):.4f}"
        )

        log_memory_usage("numerical_sparse_converted")

        logger.debug("🔮 Concatenating text and numerical features...")
        hstack_start = time.time()

        features = sp.hstack([txt_feat, num_feat], format="csr")
        hstack_time = time.time() - hstack_start

        logger.debug(f"🔮 Features concatenated in {hstack_time:.4f}s")
        logger.debug(f"🔮 Final feature matrix shape: {features.shape}")
        logger.debug(
            f"🔮 Final feature matrix density: {features.nnz / (features.shape[0] * features.shape[1]):.4f}"
        )
        logger.debug(
            f"🔮 Final feature matrix memory: {features.data.nbytes + features.indices.nbytes + features.indptr.nbytes} bytes"
        )

        log_memory_usage("final_features_ready")

        logger.debug("🔮 Running model prediction...")
        predict_start = time.time()

        prediction_probs = model_artifact["model"].predict(features)
        predict_time = time.time() - predict_start

        logger.debug(f"🔮 Model prediction completed in {predict_time:.4f}s")
        logger.debug(f"🔮 Prediction probabilities shape: {prediction_probs.shape}")
        logger.debug(f"🔮 Prediction probabilities type: {type(prediction_probs)}")

        pred_stats = {
            "min": float(prediction_probs.min()),
            "max": float(prediction_probs.max()),
            "mean": float(prediction_probs.mean()),
            "std": float(prediction_probs.std()),
        }
        logger.debug(f"🔮 Prediction statistics: {pred_stats}")

        threshold = 0.5
        above_threshold = (prediction_probs > threshold).sum()
        logger.debug(
            f"🔮 Predictions above {threshold}: {above_threshold}/{len(prediction_probs)} ({above_threshold/len(prediction_probs)*100:.2f}%)"
        )

        prediction_time = time.time() - prediction_start
        logger.debug(f"🔮 PREDICTION COMPLETE: Total time {prediction_time:.4f}s")

        logger.debug(f"🔮 Time breakdown:")
        logger.debug(
            f"   🔮 Text features: {text_time:.4f}s ({text_time/prediction_time*100:.1f}%)"
        )
        logger.debug(
            f"   🔮 URL features: {url_time:.4f}s ({url_time/prediction_time*100:.1f}%)"
        )
        logger.debug(
            f"   🔮 Structural features: {struct_time:.4f}s ({struct_time/prediction_time*100:.1f}%)"
        )
        logger.debug(
            f"   🔮 Content features: {content_time:.4f}s ({content_time/prediction_time*100:.1f}%)"
        )
        logger.debug(
            f"   🔮 Concatenation: {concat_time:.4f}s ({concat_time/prediction_time*100:.1f}%)"
        )
        logger.debug(
            f"   🔮 Sparse conversion: {sparse_time:.4f}s ({sparse_time/prediction_time*100:.1f}%)"
        )
        logger.debug(
            f"   🔮 Feature stacking: {hstack_time:.4f}s ({hstack_time/prediction_time*100:.1f}%)"
        )
        logger.debug(
            f"   🔮 Model prediction: {predict_time:.4f}s ({predict_time/prediction_time*100:.1f}%)"
        )

        log_memory_usage("prediction_end")

        logger.debug("🔮 Cleaning up intermediate objects...")
        del txt_feat, num_feat_df, num_feat, features
        gc.collect()

        log_memory_usage("prediction_cleanup")

        return prediction_probs.tolist()

    except Exception as e:
        prediction_time = time.time() - prediction_start
        logger.error(f"❌ Prediction failed after {prediction_time:.4f}s: {e}")
        logger.error(f"🔮 Prediction error traceback: {traceback.format_exc()}")
        log_memory_usage("prediction_error")
        raise


@log_timing
def main():
    """Main consumer loop with comprehensive validation."""
    global model_artifact

    logger.info("🚀 STARTUP: BlogSpy Python Consumer (PostgreSQL Queue)")
    logger.info("=" * 80)

    startup_time = time.time()
    log_memory_usage("startup")

    logger.debug(f"🚀 Python version: {sys.version}")
    logger.debug(f"🚀 Current working directory: {os.getcwd()}")
    logger.debug(f"🚀 Process ID: {os.getpid()}")
    logger.debug(f"🚀 Process name: {psutil.Process().name()}")
    logger.debug(f"🚀 Available CPU cores: {psutil.cpu_count()}")
    logger.debug(
        f"🚀 System memory: {psutil.virtual_memory().total / 1024 / 1024 / 1024:.2f} GB"
    )

    logger.info("📋 STEP 1: Environment Validation")
    env_valid, model_path = validate_environment()
    if not env_valid:
        logger.error("🚨 Environment validation failed. Exiting.")
        sys.exit(1)

    logger.info("📋 STEP 2: Database Validation")
    database_url = os.getenv(
        "DATABASE_URL", "postgresql://user:password@localhost:5432/blogspy_db"
    )
    logger.debug(f"📋 Using database URL: {database_url[:30]}...")

    db_valid, conn = validate_database_connection(database_url)
    if not db_valid:
        logger.error("🚨 Database validation failed. Exiting.")
        sys.exit(1)

    logger.info("📋 STEP 3: Model Validation")
    model_valid, model_artifact = load_and_validate_model(model_path)
    if not model_valid:
        logger.error("🚨 Model validation failed. Exiting.")
        sys.exit(1)

    initialization_time = time.time() - startup_time
    logger.info("=" * 80)
    logger.info(
        f"✅ ALL VALIDATIONS PASSED - Initialization completed in {initialization_time:.4f}s"
    )
    logger.info("🔄 Starting main processing loop")
    logger.info("=" * 80)

    log_memory_usage("initialization_complete")

    loop_iteration = 0
    total_processed = 0
    total_blogs_found = 0
    total_irrelevant = 0
    last_activity_time = time.time()

    while True:
        loop_iteration += 1
        loop_start = time.time()

        logger.debug(f"🔄 LOOP {loop_iteration}: Starting processing cycle")
        log_memory_usage(f"loop_{loop_iteration}_start")

        try:
            logger.debug(f"🔄 LOOP {loop_iteration}: Acquiring jobs from database...")
            acquisition_start = time.time()

            with conn.cursor() as cur:
                logger.debug(f"🔄 Database cursor created for loop {loop_iteration}")

                lock_query = """
                    WITH locked_jobs AS (
                        SELECT id FROM classification_queue WHERE status = 'new' ORDER BY id
                        FOR UPDATE SKIP LOCKED LIMIT %s
                    )
                    UPDATE classification_queue q SET status = 'processing', locked_at = NOW()
                    FROM locked_jobs lj WHERE q.id = lj.id
                    RETURNING q.id, q.url_id, q.payload;
                """

                logger.debug(f"🔄 Executing lock query with BATCH_SIZE={BATCH_SIZE}")
                cur.execute(lock_query, (BATCH_SIZE,))
                jobs = cur.fetchall()

                logger.debug(f"🔄 Lock query executed, fetched {len(jobs)} jobs")
                conn.commit()
                logger.debug("🔄 Transaction committed")

            acquisition_time = time.time() - acquisition_start
            logger.debug(f"🔄 Job acquisition completed in {acquisition_time:.4f}s")

            if not jobs:
                logger.debug(
                    f"🔄 LOOP {loop_iteration}: No jobs found, sleeping for {POLL_INTERVAL_SECONDS}s..."
                )
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            current_batch_size = len(jobs)
            logger.info(
                f"📦 LOOP {loop_iteration}: Processing batch of {current_batch_size} jobs..."
            )

            processing_start = time.time()

            logger.debug("📦 Extracting job data...")
            queue_ids = [job[0] for job in jobs]
            url_ids = [job[1] for job in jobs]
            payloads = [job[2] for job in jobs]

            logger.debug(f"📦 Extracted {len(queue_ids)} queue IDs")
            logger.debug(f"📦 Extracted {len(url_ids)} URL IDs")
            logger.debug(f"📦 Extracted {len(payloads)} payloads")
            logger.debug(f"📦 Queue ID range: {min(queue_ids)} to {max(queue_ids)}")
            logger.debug(f"📦 URL ID range: {min(url_ids)} to {max(url_ids)}")

            payload_sizes = [len(str(p)) for p in payloads]
            logger.debug(
                f"📦 Payload size stats: min={min(payload_sizes)}, max={max(payload_sizes)}, avg={sum(payload_sizes)/len(payload_sizes):.2f}"
            )

            logger.debug("📦 Creating DataFrame from payloads...")
            df_start = time.time()
            df = pd.DataFrame(payloads)
            df_time = time.time() - df_start

            logger.debug(f"📦 DataFrame created in {df_time:.4f}s")
            logger.debug(f"📦 DataFrame shape: {df.shape}")
            logger.debug(f"📦 DataFrame columns: {list(df.columns)}")
            logger.debug(
                f"📦 DataFrame memory usage: {df.memory_usage(deep=True).sum()} bytes"
            )

            log_memory_usage("dataframe_created")

            logger.debug("📦 Running batch prediction...")
            predictions = predict_batch(df)

            logger.debug(f"📦 Received {len(predictions)} predictions")
            logger.debug(
                f"📦 Prediction type: {type(predictions[0]) if predictions else 'N/A'}"
            )

            logger.debug("📦 Processing predictions...")
            classification_start = time.time()

            blogs_to_crawl = []
            irrelevant_jobs = []
            threshold = 0.5

            for i, (url_id, pred) in enumerate(zip(url_ids, predictions)):
                logger.debug(f"📦 Job {i+1}: URL_ID={url_id}, Prediction={pred:.4f}")

                if pred > threshold:
                    blogs_to_crawl.append((url_id,))
                    logger.debug(f"📦   -> BLOG (above {threshold})")
                else:
                    irrelevant_jobs.append((url_id,))
                    logger.debug(f"📦   -> IRRELEVANT (below {threshold})")

            classification_time = time.time() - classification_start

            blogs_count = len(blogs_to_crawl)
            irrelevant_count = len(irrelevant_jobs)

            logger.info(
                f"📦 Classification results: {blogs_count} blogs, {irrelevant_count} irrelevant"
            )
            logger.debug(f"📦 Classification completed in {classification_time:.4f}s")

            total_processed += current_batch_size
            total_blogs_found += blogs_count
            total_irrelevant += irrelevant_count

            logger.debug("📦 Updating database with results...")
            db_update_start = time.time()

            processed_queue_job_ids = [(qid,) for qid in queue_ids]
            logger.debug(
                f"📦 Prepared {len(processed_queue_job_ids)} queue job IDs for deletion"
            )

            with conn.cursor() as cur:
                logger.debug("📦 Database cursor created for updates")

                if blogs_to_crawl:
                    logger.debug(
                        f"📦 Updating {len(blogs_to_crawl)} URLs to 'pending_crawl'..."
                    )
                    update_blogs_start = time.time()
                    execute_values(
                        cur,
                        "UPDATE urls SET status = 'pending_crawl', processed_at = NOW() WHERE id IN %s",
                        blogs_to_crawl,
                    )
                    update_blogs_time = time.time() - update_blogs_start
                    logger.debug(f"📦 Blog URLs updated in {update_blogs_time:.4f}s")

                if irrelevant_jobs:
                    logger.debug(
                        f"📦 Updating {len(irrelevant_jobs)} URLs to 'irrelevant'..."
                    )
                    update_irrelevant_start = time.time()
                    execute_values(
                        cur,
                        "UPDATE urls SET status = 'irrelevant', processed_at = NOW() WHERE id IN %s",
                        irrelevant_jobs,
                    )
                    update_irrelevant_time = time.time() - update_irrelevant_start
                    logger.debug(
                        f"📦 Irrelevant URLs updated in {update_irrelevant_time:.4f}s"
                    )

                if processed_queue_job_ids:
                    logger.debug(
                        f"📦 Deleting {len(processed_queue_job_ids)} processed jobs from queue..."
                    )
                    delete_queue_start = time.time()
                    execute_values(
                        cur,
                        "DELETE FROM classification_queue WHERE id IN %s",
                        processed_queue_job_ids,
                    )
                    delete_queue_time = time.time() - delete_queue_start
                    logger.debug(f"📦 Queue jobs deleted in {delete_queue_time:.4f}s")

            logger.debug("📦 Committing database transaction...")
            commit_start = time.time()
            conn.commit()
            commit_time = time.time() - commit_start
            logger.debug(f"📦 Transaction committed in {commit_time:.4f}s")

            db_update_time = time.time() - db_update_start
            processing_time = time.time() - processing_start
            loop_time = time.time() - loop_start

            last_activity_time = time.time()

            logger.info(
                f"✅ LOOP {loop_iteration} COMPLETE: Processed {current_batch_size} jobs in {loop_time:.4f}s"
            )
            logger.info(
                f"   📊 Results: {blogs_count} blogs, {irrelevant_count} irrelevant"
            )
            logger.info(
                f"   ⏱️  Timings: DB acquisition={acquisition_time:.3f}s, processing={processing_time:.3f}s, DB update={db_update_time:.3f}s"
            )

            uptime = time.time() - startup_time
            avg_processing_rate = total_processed / uptime

            logger.info(
                f"   📈 Totals: {total_processed} processed, {total_blogs_found} blogs, {total_irrelevant} irrelevant"
            )
            logger.info(
                f"   📈 Rates: {avg_processing_rate:.2f} jobs/sec average, uptime {uptime:.1f}s"
            )

            log_memory_usage(f"loop_{loop_iteration}_complete")

        except psycopg2.OperationalError as e:
            loop_time = time.time() - loop_start
            logger.error(
                f"💥 LOOP {loop_iteration}: Database connection lost after {loop_time:.4f}s: {e}"
            )
            logger.debug(f"💥 Database error traceback: {traceback.format_exc()}")

            if conn:
                try:
                    conn.close()
                    logger.debug("💥 Closed old database connection")
                except:
                    pass

            logger.info("💥 Attempting to reconnect to database in 5 seconds...")
            time.sleep(5)

            try:
                reconnect_start = time.time()
                conn = psycopg2.connect(database_url)
                reconnect_time = time.time() - reconnect_start
                logger.info(
                    f"✅ Database reconnected successfully in {reconnect_time:.4f}s"
                )
                log_memory_usage("database_reconnected")
            except Exception as reconn_e:
                logger.error(f"❌ Reconnection failed: {reconn_e}")
                logger.debug(
                    f"❌ Reconnection error traceback: {traceback.format_exc()}"
                )

        except Exception as e:
            loop_time = time.time() - loop_start
            logger.error(
                f"💥 LOOP {loop_iteration}: Unexpected error after {loop_time:.4f}s: {e}"
            )
            logger.error(f"💥 Error traceback: {traceback.format_exc()}")

            if conn:
                try:
                    logger.debug("💥 Rolling back database transaction...")
                    conn.rollback()
                    logger.debug("💥 Transaction rolled back successfully")
                except Exception as rollback_e:
                    logger.error(f"💥 Failed to rollback transaction: {rollback_e}")

            logger.info("💥 Waiting 5 seconds before retrying...")
            time.sleep(5)
            log_memory_usage(f"loop_{loop_iteration}_error")

        if loop_iteration % 10 == 0:
            uptime = time.time() - startup_time
            inactive_time = time.time() - last_activity_time

            logger.info(f"📊 PERIODIC STATS (Loop {loop_iteration}):")
            logger.info(
                f"   📊 Uptime: {uptime:.1f}s, Last activity: {inactive_time:.1f}s ago"
            )
            logger.info(f"   📊 Total processed: {total_processed} jobs")
            logger.info(
                f"   📊 Success rate: {(total_blogs_found + total_irrelevant) / max(total_processed, 1) * 100:.2f}%"
            )
            logger.info(
                f"   📊 Blog detection rate: {total_blogs_found / max(total_processed, 1) * 100:.2f}%"
            )

            log_memory_usage(f"periodic_stats_{loop_iteration}")

            if loop_iteration % 100 == 0:
                logger.debug(f"🧹 Forcing garbage collection at loop {loop_iteration}")
                gc_start = time.time()
                collected = gc.collect()
                gc_time = time.time() - gc_start
                logger.debug(
                    f"🧹 Garbage collection completed in {gc_time:.4f}s, collected {collected} objects"
                )
                log_memory_usage(f"post_gc_{loop_iteration}")


if __name__ == "__main__":
    main()
