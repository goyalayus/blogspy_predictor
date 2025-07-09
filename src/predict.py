# src/predict.py

from src.utils import setup_logging
from src.config import MODELS_DIR, ID_TO_LABEL, REQUEST_TIMEOUT, REQUEST_HEADERS
from src.feature_engineering import extract_url_features, extract_structural_features, extract_content_features
import sys
import pathlib
import argparse
import joblib
import pandas as pd
import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import warnings
import scipy.sparse as sp
import time

# --- Path and Logger Setup ---
project_root = pathlib.Path(__file__).parent.parent
sys.path.append(str(project_root))
logger = setup_logging('predict')


def fetch_and_parse(url: str) -> dict | None:
    """ Fetches and parses a URL, returning a dictionary of contents. """
    logger.info(f"Fetching content from {url}...")
    fetch_start_time = time.time()
    try:
        response = requests.get(
            url, timeout=REQUEST_TIMEOUT, headers=REQUEST_HEADERS)
        response.raise_for_status()
        html_content = response.text

        warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
        soup = BeautifulSoup(html_content, 'lxml')
        for script_or_style in soup(['script', 'style']):
            script_or_style.decompose()
        text_content = ' '.join(soup.stripped_strings)

        fetch_duration = time.time() - fetch_start_time
        logger.debug("Content fetched successfully", extra_data={
            "event": "fetch_success", "url": url, "duration_s": fetch_duration
        })

        return {"url": url, "html_content": html_content, "text_content": text_content}
    except requests.RequestException as e:
        logger.error(f"Failed to fetch or process {url}: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred while parsing {url}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Classify websites.")
    parser.add_argument('urls', nargs='+',
                        help="One or more full URLs to classify")
    parser.add_argument('--model', type=str, default='main',
                        help="The model to use (e.g., 'main').")
    args = parser.parse_args()

    model_filename = 'lgbm_final_model.joblib'
    model_path = MODELS_DIR / model_filename

    if not model_path.exists():
        logger.error(
            f"Model file not found at {model_path}. Please ensure it is in the correct directory.")
        sys.exit(1)

    logger.info(f"Loading model: {model_filename}...")
    artifact = joblib.load(model_path)
    logger.info("Model loaded successfully.")

    for url in args.urls:
        print("-" * 50)
        logger.info(f"Analyzing URL: {url}")

        site_data = fetch_and_parse(url)
        if site_data is None:
            print(f"  ❌ Could not analyze {url}. Skipping.")
            continue

        try:
            predict_start_time = time.time()
            vectorizer = artifact['vectorizer']
            model = artifact['model']

            df = pd.DataFrame([site_data])

            txt_features = vectorizer.transform(df["text_content"].fillna(""))
            url_features = extract_url_features(
                df["url"]).to_numpy(dtype="float32")
            structural_features = extract_structural_features(
                df["html_content"]).to_numpy(dtype="float32")
            content_features = extract_content_features(
                df["text_content"]).to_numpy(dtype="float32")

            features = sp.hstack([
                txt_features, sp.csr_matrix(url_features),
                sp.csr_matrix(structural_features), sp.csr_matrix(
                    content_features)
            ], format="csr")

            probabilities = model.predict(features)
            prediction_id = (probabilities > 0.5).astype(int)[0]
            confidence = probabilities[0] if prediction_id == 1 else 1 - \
                probabilities[0]

            predict_duration = time.time() - predict_start_time
            logger.debug("Prediction complete", extra_data={
                "event": "predict_success", "url": url, "duration_s": predict_duration
            })

            label = ID_TO_LABEL[prediction_id]
            print(f"\n✅ Results for: {url}")
            print(f"  Prediction: {label.upper()}")
            print(f"  Confidence: {confidence:.2%}")

        except Exception as e:
            logger.error(
                f"An error occurred during prediction for {url}: {e}", exc_info=True)
            print(f"  ❌ Failed to predict for {url}.")

    print("-" * 50)


if __name__ == "__main__":
    main()
