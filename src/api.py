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

# --- Project Root Setup ---
project_root = pathlib.Path(__file__).parent.parent
sys.path.append(str(project_root.resolve()))

from src.feature_engineering import (
    extract_url_features,
    extract_structural_features,
    extract_content_features,
)
from src.config import MODELS_DIR

# --- NEW: Structured Logging Configuration ---
LOG_FILE = os.getenv("LOG_FILE", "logs/python_api.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Ensure the log directory exists
os.makedirs(pathlib.Path(LOG_FILE).parent, exist_ok=True)

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {
            "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
            "format": "%(asctime)s %(levelname)s %(name)s %(message)s",
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
            "maxBytes": 10485760,  # 10 MB
            "backupCount": 5,
            "encoding": "utf-8",
        },
    },
    "loggers": {
        "uvicorn": {"handlers": ["console", "file"], "level": LOG_LEVEL},
        "BlogSpyAPI": {"handlers": ["console", "file"], "level": LOG_LEVEL, "propagate": False},
        "FeatureEngineering": {"handlers": ["console", "file"], "level": LOG_LEVEL, "propagate": False},
    },
    "root": {"handlers": ["console", "file"], "level": LOG_LEVEL},
}

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger("BlogSpyAPI")


MODEL_PATH_STR = os.getenv("MODEL_PATH", str(MODELS_DIR / "lgbm_final_model.joblib"))
MODEL_PATH = pathlib.Path(MODEL_PATH_STR)


def _run_prediction_pipeline(
    data: "PredictionRequest", artifact: dict, logger_adapter: logging.LoggerAdapter
) -> tuple[bool, float]:
    """
    Encapsulates the full feature engineering and prediction pipeline.
    """
    pipeline_start = time.perf_counter()
    df = pd.DataFrame([data.dict()])

    # --- Text Vectorization ---
    vec_start = time.perf_counter()
    txt_feat = artifact["vectorizer"].transform(df["text_content"].fillna(""))
    vec_duration = (time.perf_counter() - vec_start) * 1000
    logger_adapter.info("Completed text vectorization", extra={
        "event": {"name": "TEXT_VECTORIZATION_COMPLETED", "stage": "end", "duration_ms": round(vec_duration, 2)},
        "details": {
            "input": {"text_content_len": len(data.text_content)},
            "output": {"vector_shape": list(txt_feat.shape)}
        }
    })

    # --- Feature Engineering ---
    feat_start = time.perf_counter()
    url_feats = extract_url_features(df["url"])
    struct_feats = extract_structural_features(df["html_content"])
    content_feats = extract_content_features(df["text_content"])
    feat_duration = (time.perf_counter() - feat_start) * 1000
    logger_adapter.info("Completed feature engineering", extra={
        "event": {"name": "FEATURE_EXTRACTION_COMPLETED", "stage": "end", "duration_ms": round(feat_duration, 2)},
        "details": {
            # Not logging full features, just confirmation they were built
            "output": {
                "url_features_shape": list(url_feats.shape),
                "structural_features_shape": list(struct_feats.shape),
                "content_features_shape": list(content_feats.shape),
            }
        }
    })

    num_feat_df = pd.concat([url_feats, struct_feats, content_feats], axis=1)
    num_feat = sp.csr_matrix(num_feat_df.to_numpy(dtype="float32"))
    features = sp.hstack([txt_feat, num_feat], format="csr")

    # --- Model Prediction ---
    pred_start = time.perf_counter()
    prediction_prob = artifact["model"].predict(features)[0]
    pred_duration = (time.perf_counter() - pred_start) * 1000
    logger_adapter.info("Completed model prediction", extra={
        "event": {"name": "MODEL_PREDICTION_COMPLETED", "stage": "end", "duration_ms": round(pred_duration, 2)},
        "details": {
            "input": {"feature_vector_shape": list(features.shape)},
            "output": {"raw_probability": float(prediction_prob)}
        }
    })

    is_personal_blog = bool(prediction_prob > 0.5)

    pipeline_duration = (time.perf_counter() - pipeline_start) * 1000
    logger_adapter.info("Prediction pipeline completed", extra={
        "event": {"name": "PREDICTION_PIPELINE_COMPLETED", "stage": "end", "duration_ms": round(pipeline_duration, 2)},
        "details": {
            "input": {"url": data.url},
            "output": {"is_personal_blog": is_personal_blog, "prediction_prob": float(prediction_prob)}
        }
    })

    return is_personal_blog, float(prediction_prob)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Handles application startup. It will load the model and fail fast if it can't.
    """
    logger.info("--- Application startup sequence initiated ---")
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
            msg = "FATAL: Model artifact is invalid. Missing 'vectorizer' or 'model' key."
            logger.error(msg, extra={"event": {"name": "MODEL_LOADING_FAILED"}})
            raise ValueError(msg)
        
        logger.info("Model artifact loaded successfully", extra={
            "event": {"name": "MODEL_LOADING_COMPLETED", "stage": "end", "duration_ms": round(load_duration, 2)},
            "details": {"input": {"model_path": app.state.model_path}}
        })

    except Exception as e:
        load_duration = (time.perf_counter() - load_start) * 1000
        logger.critical(
            "FATAL: Failed to load model artifact.",
            exc_info=True,
            extra={
                "event": {"name": "MODEL_LOADING_FAILED", "stage": "end", "duration_ms": round(load_duration, 2)},
                "details": {"error": {"message": str(e)}}
            }
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


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Logs incoming requests and their processing time with a unique correlation ID."""
    request_id = str(uuid.uuid4())
    # Make request_id available to the rest of the app
    request.state.request_id = request_id

    start_time = time.time()
    response = await call_next(request)
    process_time = (time.time() - start_time) * 1000
    
    logger.info("Processed incoming HTTP request", extra={
        "service": "python-api",
        "correlation_id": request_id,
        "event": {"name": "HTTP_REQUEST_COMPLETED", "stage": "end", "duration_ms": round(process_time, 2)},
        "details": {
            "input": {
                "method": request.method,
                "path": request.url.path,
                "client_ip": request.client.host,
            },
            "output": {"status_code": response.status_code}
        }
    })
    return response


class PredictionRequest(BaseModel):
    url: str = Field(
        ...,
        description="The final URL of the page after any redirects.",
        example="https://jvns.ca/",
    )
    html_content: str = Field(..., description="The full raw HTML of the page.")
    text_content: str = Field(
        ..., description="The extracted, cleaned plain text from the page."
    )


class PredictionResponse(BaseModel):
    is_personal_blog: bool
    confidence: float = Field(
        ..., ge=0, le=1, description="The model's confidence score (probability)."
    )
    model_path: str


@app.post("/predict", response_model=PredictionResponse)
def predict(request: PredictionRequest, http_request: Request):
    """
    Analyzes the provided URL and content to predict its classification.
    """
    request_id = http_request.state.request_id
    # Use a logger adapter to automatically add the correlation_id to all log records
    logger_adapter = logging.LoggerAdapter(logger, {"correlation_id": request_id, "service": "python-api"})
    
    logger_adapter.info("Prediction request received", extra={
        "event": {"name": "PREDICTION_PIPELINE_STARTED", "stage": "start"},
        "details": {
            "input": {
                "url": request.url,
                "html_content_len": len(request.html_content),
                "text_content_len": len(request.text_content)
            }
        }
    })

    try:
        is_blog, probability = _run_prediction_pipeline(request, app.state.artifact, logger_adapter)

        confidence = probability if is_blog else 1 - probability

        return PredictionResponse(
            is_personal_blog=is_blog,
            confidence=confidence,
            model_path=app.state.model_path,
        )

    except Exception as e:
        logger_adapter.error(
            "Prediction pipeline failed with an exception", 
            exc_info=True, 
            extra={
                "event": {"name": "PREDICTION_PIPELINE_FAILED"},
                "details": {"error": {"message": str(e)}}
            }
        )
        raise HTTPException(
            status_code=500,
            detail=f"An internal error occurred during prediction: {e}",
        )


@app.get("/health", status_code=200, summary="Performs a health check on the service.")
def health_check():
    """
    Checks if the service is running and if the model is successfully loaded.
    """
    model_loaded = hasattr(app.state, "artifact") and app.state.artifact is not None
    return {
        "status": "ok" if model_loaded else "degraded",
        "model_loaded": model_loaded,
        "model_path_configured": app.state.model_path
        if hasattr(app.state, "model_path")
        else "Not configured",
    }


if __name__ == "__main__":
    uvicorn.run("src.api:app", host="0.0.0.0", port=8000, reload=True)
