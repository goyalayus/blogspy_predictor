# src/api.py

import joblib
import pandas as pd
import scipy.sparse as sp
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
import pathlib
import sys
import os
import time
import logging
import logging.config
import uuid
from contextlib import asynccontextmanager
from pythonjsonlogger import jsonlogger
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Histogram
import sklearn, lightgbm, platform, hashlib

# --- MODIFICATION START: Add new imports ---
import requests
from bs4 import BeautifulSoup
# --- MODIFICATION END ---


project_root = pathlib.Path(__file__).parent.parent
sys.path.append(str(project_root.resolve()))

from src.feature_engineering import (
    extract_url_features,
    extract_structural_features,
    extract_content_features,
)
from src.config import MODELS_DIR, REQUEST_HEADERS, REQUEST_TIMEOUT # Import headers and timeout

# --- METRICS ---
# ... (this section remains the same)
ML_PIPELINE_DURATION = Histogram(
    "blogspy_ml_pipeline_duration_seconds",
    "Total duration of the ML prediction pipeline."
)

ML_FEATURE_ENGINEERING_DURATION = Histogram(
    "blogspy_ml_feature_engineering_duration_seconds",
    "Duration of specific feature engineering steps.",
    ["step"]
)

# --- LOGGING CONFIG ---
# ... (this section remains the same)
LOG_FILE = os.getenv("LOG_FILE", "logs/python_api.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

os.makedirs(pathlib.Path(LOG_FILE).parent, exist_ok=True)

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {
            "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "json",
            "stream": "ext://sys.stdout",
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "json",
            "filename": LOG_FILE,
            "maxBytes": 10485760,
            "backupCount": 5,
            "encoding": "utf-8",
        },
    },
    "loggers": {
        "uvicorn": {"handlers": ["console", "file"], "level": LOG_LEVEL},
        "BlogSpyAPI": {
            "handlers": ["console", "file"],
            "level": LOG_LEVEL,
            "propagate": False,
        },
        "FeatureEngineering": {
            "handlers": ["console", "file"],
            "level": LOG_LEVEL,
            "propagate": False,
        },
    },
    "root": {"handlers": ["console", "file"], "level": LOG_LEVEL},
}

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger("BlogSpyAPI")


# --- MODEL PATH ---
# ... (this section remains the same)
MODEL_PATH_STR = os.getenv("MODEL_PATH", str(MODELS_DIR / "lgbm_final_model.joblib"))
MODEL_PATH = pathlib.Path(MODEL_PATH_STR)


# --- MODIFICATION START: Create a dedicated fetch/parse function ---
def _fetch_and_parse(url: str, log_context: dict) -> tuple[str, str, str]:
    """Fetches, parses, and extracts text from a URL."""
    try:
        response = requests.get(url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        html_content = response.text
        final_url = response.url # Use the URL after redirects
    except requests.exceptions.RequestException as e:
        logger.error(
            "Failed to fetch URL content",
            exc_info=True,
            extra={**log_context, "details": {"error": str(e), "url": url}},
        )
        raise HTTPException(status_code=400, detail=f"Failed to fetch URL: {url}. Reason: {e}")

    soup = BeautifulSoup(html_content, 'lxml')
    for script_or_style in soup(["script", "style"]):
        script_or_style.decompose()
    text_content = " ".join(soup.stripped_strings)

    logger.info("Content fetched and parsed successfully", extra={**log_context, "details": {"final_url": final_url}})
    return final_url, html_content, text_content
# --- MODIFICATION END ---


@ML_PIPELINE_DURATION.time()
def _run_prediction_pipeline(
    # --- MODIFICATION START: Change input parameters ---
    request_url: str,
    artifact: dict,
    log_context: dict
    # --- MODIFICATION END ---
) -> tuple[bool, float]:
    """
    Encapsulates the full fetch, feature engineering, and prediction pipeline.
    """
    def log_info(message: str, extra: dict):
        merged = {**log_context, **extra}
        logger.info(message, extra=merged)

    pipeline_start = time.perf_counter()

    # --- MODIFICATION START: Call the new fetch function ---
    with ML_FEATURE_ENGINEERING_DURATION.labels(step="fetch_and_parse").time():
        final_url, html_content, text_content = _fetch_and_parse(request_url, log_context)
    
    df = pd.DataFrame([{
        "url": final_url,
        "html_content": html_content,
        "text_content": text_content
    }])
    # --- MODIFICATION END ---

    # Text vectorization
    with ML_FEATURE_ENGINEERING_DURATION.labels(step="vectorization").time():
        txt_feat = artifact["vectorizer"].transform(df["text_content"].fillna(""))

    # ... (the rest of this function remains exactly the same)
    log_info(
        "Text feature fingerprint",
        extra={
            "details": {
                "txt_nnz": int(txt_feat.nnz),
                "txt_sum": float(txt_feat.sum()),
                "txt_shape": list(txt_feat.shape),
            }
        },
    )

    vec_duration = (time.perf_counter() - pipeline_start) * 1000
    log_info(
        "Completed text vectorization",
        extra={
            "event": {
                "name": "TEXT_VECTORIZATION_COMPLETED",
                "stage": "end",
                "duration_ms": round(vec_duration, 2),
            },
            "details": {
                "input": {"text_content_len": len(text_content)},
                "output": {"vector_shape": list(txt_feat.shape)},
            },
        },
    )

    # Feature engineering
    feat_start = time.perf_counter()
    with ML_FEATURE_ENGINEERING_DURATION.labels(step="url_features").time():
        url_feats = extract_url_features(df["url"])
    with ML_FEATURE_ENGINEERING_DURATION.labels(step="structural_features").time():
        struct_feats = extract_structural_features(df["html_content"])
    with ML_FEATURE_ENGINEERING_DURATION.labels(step="content_features").time():
        content_feats = extract_content_features(df["text_content"])

    feat_duration = (time.perf_counter() - feat_start) * 1000
    log_info(
        "Completed feature engineering",
        extra={
            "event": {
                "name": "FEATURE_EXTRACTION_COMPLETED",
                "stage": "end",
                "duration_ms": round(feat_duration, 2),
            },
            "details": {
                "output": {
                    "url_features_shape": list(url_feats.shape),
                    "structural_features_shape": list(struct_feats.shape),
                    "content_features_shape": list(content_feats.shape),
                }
            },
        },
    )

    # Assemble features
    num_feat_df = pd.concat([url_feats, struct_feats, content_feats], axis=1)
    num_feat = sp.csr_matrix(num_feat_df.to_numpy(dtype="float32"))
    features = sp.hstack([txt_feat, num_feat], format="csr")

    # Feature fingerprint for final features
    log_info(
        "Final feature fingerprint",
        extra={
            "details": {
                "num_feat_sum": float(num_feat.sum()),
                "final_nnz": int(features.nnz),
                "final_shape": list(features.shape),
            }
        },
    )

    # Prediction probability: support sklearn classifiers and LightGBM Booster
    pred_start = time.perf_counter()
    with ML_FEATURE_ENGINEERING_DURATION.labels(step="model_prediction").time():
        # Prediction probability: support sklearn classifiers and LightGBM Booster
        model = artifact["model"]
        try:
            proba = model.predict_proba(features)
            if getattr(proba, "ndim", 1) == 2:
                prediction_prob = float(proba[0, 1])
            else:
                prediction_prob = float(proba[0])
        except AttributeError:
            preds = model.predict(features)  # LightGBM Booster returns prob for binary
            if getattr(preds, "ndim", 1) == 2:
                prediction_prob = float(preds[0, 1])
            else:
                prediction_prob = float(preds[0])

    pred_duration = (time.perf_counter() - pred_start) * 1000
    log_info(
        "Completed model prediction",
        extra={
            "event": {
                "name": "MODEL_PREDICTION_COMPLETED",
                "stage": "end",
                "duration_ms": round(pred_duration, 2),
            },
            "details": {
                "input": {"feature_vector_shape": list(features.shape)},
                "output": {"raw_probability": float(prediction_prob)},
            },
        },
    )

    # Thresholding
    is_personal_blog = bool(prediction_prob > 0.75)

    pipeline_duration = (time.perf_counter() - pipeline_start) * 1000
    log_info(
        "Prediction pipeline completed",
        extra={
            "event": {
                "name": "PREDICTION_PIPELINE_COMPLETED",
                "stage": "end",
                "duration_ms": round(pipeline_duration, 2),
            },
            "details": {
                "input": {"url": request_url},
                "output": {
                    "is_personal_blog": is_personal_blog,
                    "prediction_prob": float(prediction_prob),
                },
            },
        },
    )

    return is_personal_blog, float(prediction_prob)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ... (this entire function remains the same)
    logger.info("--- Application startup sequence initiated ---")

    # Log versions
    logger.info(
        "Runtime versions",
        extra={"details": {
            "python": sys.version,
            "platform": platform.platform(),
            "sklearn": sklearn.__version__,
            "lightgbm": lightgbm.__version__,
        }}
    )

    # Expose Prometheus /metrics endpoint
    instrumentator.expose(app, include_in_schema=False)

    app.state.model_path = str(MODEL_PATH.resolve())

    if not MODEL_PATH.exists():
        msg = f"FATAL: Model file not found at path: {app.state.model_path}"
        logger.error(msg, extra={"event": {"name": "MODEL_LOADING_FAILED"}})
        raise RuntimeError(msg)

    logger.info(f"Attempting to load model artifact from: {app.state.model_path}")
    load_start = time.perf_counter()
    try:
        app.state.artifact = joblib.load(MODEL_PATH)
        load_duration = (time.perf_counter() - load_start) * 1000

        if "vectorizer" not in app.state.artifact or "model" not in app.state.artifact:
            msg = (
                "FATAL: Model artifact is invalid. Missing 'vectorizer' or 'model' key."
            )
            logger.error(msg, extra={"event": {"name": "MODEL_LOADING_FAILED"}})
            raise ValueError(msg)

        logger.info(
            "Model artifact loaded successfully",
            extra={
                "event": {
                    "name": "MODEL_LOADING_COMPLETED",
                    "stage": "end",
                    "duration_ms": round(load_duration, 2),
                },
                "details": {"input": {"model_path": app.state.model_path}},
            },
        )

    except Exception as e:
        load_duration = (time.perf_counter() - load_start) * 1000
        logger.critical(
            "FATAL: Failed to load model artifact.",
            exc_info=True,
            extra={
                "event": {
                    "name": "MODEL_LOADING_FAILED",
                    "stage": "end",
                    "duration_ms": round(load_duration, 2),
                },
                "details": {"error": {"message": str(e)}},
            },
        )
        raise RuntimeError(f"Could not load model artifact: {e}") from e

    logger.info("--- Application startup sequence complete ---")
    yield
    logger.info("--- Application shutdown sequence initiated ---")
    app.state.artifact = None
    logger.info("Model artifact cleared from memory.")


app = FastAPI(
    title="BlogSpy ML Prediction Service",
    description="Provides predictions for whether a URL points to a personal blog or a corporate/SEO site.",
    version="2.0.0",
    lifespan=lifespan,
)

# Prometheus instrumentation
instrumentator = Instrumentator().instrument(app)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    # ... (this entire function remains the same)
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id

    start_time = time.time()
    response = await call_next(request)
    process_time = (time.time() - start_time) * 1000

    logger.info(
        "Processed incoming HTTP request",
        extra={
            "service": "python-api",
            "correlation_id": request_id,
            "event": {
                "name": "HTTP_REQUEST_COMPLETED",
                "stage": "end",
                "duration_ms": round(process_time, 2),
            },
            "details": {
                "input": {
                    "method": request.method,
                    "path": request.url.path,
                    "client_ip": request.client.host,
                },
                "output": {"status_code": response.status_code},
            },
        },
    )
    return response

# --- MODIFICATION START: The input model is now simpler ---
class PredictionRequest(BaseModel):
    url: str = Field(
        ...,
        description="The URL of the page to fetch and classify.",
        example="https://jvns.ca/",
    )
# --- MODIFICATION END ---

class PredictionResponse(BaseModel):
    is_personal_blog: bool
    confidence: float = Field(
        ..., ge=0, le=1, description="The model's confidence score (probability)."
    )
    model_path: str


@app.post("/predict", response_model=PredictionResponse)
def predict(request: PredictionRequest, http_request: Request):
    """
    Fetches content from the provided URL and analyzes it to predict its classification.
    """
    request_id = http_request.state.request_id
    base_extra = {"correlation_id": request_id, "service": "python-api"}

    logger.info(
        "Prediction request received",
        extra={
            **base_extra,
            "event": {"name": "PREDICTION_PIPELINE_STARTED", "stage": "start"},
            # --- MODIFICATION START: Log less input data ---
            "details": {
                "input": { "url": request.url }
            },
            # --- MODIFICATION END ---
        },
    )

    try:
        # --- MODIFICATION START: Pass only the URL to the pipeline ---
        is_blog, probability = _run_prediction_pipeline(
            request.url, app.state.artifact, base_extra
        )
        # --- MODIFICATION END ---

        confidence = probability if is_blog else 1 - probability

        return PredictionResponse(
            is_personal_blog=is_blog,
            confidence=confidence,
            model_path=app.state.model_path,
        )

    except Exception as e:
        logger.error(
            "Prediction pipeline failed with an exception",
            exc_info=True,
            extra={
                **base_extra,
                "event": {"name": "PREDICTION_PIPELINE_FAILED"},
                "details": {"error": {"message": str(e)}},
            },
        )
        # If the exception is an HTTPException we raised, re-raise it.
        if isinstance(e, HTTPException):
            raise e
        # Otherwise, wrap it in a generic 500 error.
        raise HTTPException(
            status_code=500,
            detail=f"An internal error occurred during prediction: {e}",
        )


@app.get("/health", status_code=200, summary="Performs a health check on the service.")
def health_check():
    # ... (this entire function remains the same)
    model_loaded = hasattr(app.state, "artifact") and app.state.artifact is not None
    return {
        "status": "ok" if model_loaded else "degraded",
        "model_loaded": model_loaded,
        "model_path_configured": (
            app.state.model_path
            if hasattr(app.state, "model_path")
            else "Not configured"
        ),
    }


if __name__ == "__main__":
    uvicorn.run("src.api:app", host="0.0.0.0", port=8000, reload=True)
