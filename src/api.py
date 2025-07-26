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
from contextlib import asynccontextmanager

project_root = pathlib.Path(__file__).parent.parent
sys.path.append(str(project_root.resolve()))

from src.feature_engineering import (
    extract_url_features,
    extract_structural_features,
    extract_content_features,
)
from src.config import MODELS_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("BlogSpyAPI")

MODEL_PATH_STR = os.getenv("MODEL_PATH", str(MODELS_DIR / "lgbm_final_model.joblib"))
MODEL_PATH = pathlib.Path(MODEL_PATH_STR)


def _run_prediction_pipeline(
    data: "PredictionRequest", artifact: dict
) -> tuple[bool, float]:
    """
    Encapsulates the full feature engineering and prediction pipeline.

    Returns:
        A tuple containing (is_personal_blog, confidence_probability)
    """
    df = pd.DataFrame([data.dict()])

    txt_feat = artifact["vectorizer"].transform(df["text_content"].fillna(""))

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

    prediction_prob = artifact["model"].predict(features)[0]
    is_personal_blog = bool(prediction_prob > 0.5)

    return is_personal_blog, float(prediction_prob)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Handles application startup. It will load the model and fail fast if it can't.
    """
    logger.info("--- Application startup sequence initiated ---")
    app.state.model_path = str(MODEL_PATH.resolve())

    if not MODEL_PATH.exists():
        logger.error(
            f"FATAL: Model file not found at the specified path: {app.state.model_path}"
        )
        raise RuntimeError(f"Model file not found: {app.state.model_path}")

    logger.info(f"Attempting to load model artifact from: {app.state.model_path}")
    try:
        app.state.artifact = joblib.load(MODEL_PATH)

        if "vectorizer" not in app.state.artifact or "model" not in app.state.artifact:
            logger.error(
                "FATAL: Model artifact is invalid. Missing 'vectorizer' or 'model' key."
            )
            raise ValueError("Invalid model artifact structure.")

        logger.info("✅ Model artifact loaded and verified successfully.")

    except Exception as e:
        logger.error(
            f"FATAL: Failed to load the model from {app.state.model_path}.",
            exc_info=True,
        )
        logger.error(
            "This is often caused by a mismatch in library versions (e.g., scikit-learn, lightgbm) "
            "between the environment that saved the model and the one trying to load it."
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
    """Logs incoming requests and their processing time."""
    start_time = time.time()
    response = await call_next(request)
    process_time = (time.time() - start_time) * 1000
    logger.info(
        f"Request: {request.method} {request.url.path} | Status: {response.status_code} | Duration: {process_time:.2f}ms"
    )
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
def predict(request: PredictionRequest):
    """
    Analyzes the provided URL and content to predict its classification.
    """
    logger.info(f"Prediction requested for URL: {request.url}")

    try:
        is_blog, probability = _run_prediction_pipeline(request, app.state.artifact)

        confidence = probability if is_blog else 1 - probability

        return PredictionResponse(
            is_personal_blog=is_blog,
            confidence=confidence,
            model_path=app.state.model_path,
        )

    except Exception as e:
        logger.error(f"Prediction failed for URL {request.url}", exc_info=True)
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
    uvicorn.run(app, host="0.0.0.0", port=8000)
