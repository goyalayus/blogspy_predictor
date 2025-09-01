# test.py (modified to accept command-line arguments)

import sys
import pathlib
import requests
import joblib
import pandas as pd
import scipy.sparse as sp
from bs4 import BeautifulSoup
import argparse # <-- New import

# --- Setup Project Paths ---
project_root = pathlib.Path(__file__).parent
src_path = project_root / "src"
sys.path.append(str(project_root.resolve()))

from src.config import MODELS_DIR, REQUEST_HEADERS
from src.feature_engineering import (
    extract_url_features,
    extract_structural_features,
    extract_content_features,
)

def run_prediction_for_url(target_url, model, vectorizer):
    """
    Runs the full fetch, feature engineering, and prediction pipeline
    for a single URL and prints the results.
    """
    print(f"\n--- Analyzing: {target_url} ---")

    # 1. FETCH CONTENT
    try:
        response = requests.get(target_url, headers=REQUEST_HEADERS, timeout=10)
        response.raise_for_status()
        html_content = response.text
    except requests.RequestException as e:
        print(f"❌ Failed to fetch URL: {e}")
        return

    # 2. PARSE & CLEAN CONTENT
    soup = BeautifulSoup(html_content, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text_content = " ".join(soup.get_text().split())

    # 3. RUN PREDICTION PIPELINE
    data = pd.DataFrame([{
        "url": target_url,
        "html_content": html_content,
        "text_content": text_content,
    }])

    txt_feat = vectorizer.transform(data["text_content"])
    url_feats = extract_url_features(data["url"])
    struct_feats = extract_structural_features(data["html_content"])
    content_feats = extract_content_features(data["text_content"])

    num_feat_df = pd.concat([url_feats, struct_feats, content_feats], axis=1)
    num_feat = sp.csr_matrix(num_feat_df.to_numpy(dtype="float32"))
    features = sp.hstack([txt_feat, num_feat], format="csr")

    prediction_prob = model.predict(features)[0]

    # 4. DISPLAY RESULTS
    is_personal_blog = prediction_prob > 0.5
    prediction_label = "PERSONAL_BLOG" if is_personal_blog else "CORPORATE_SEO"
    confidence = prediction_prob if is_personal_blog else 1 - prediction_prob

    print("--------------------------------------------------")
    print(f"✅ Results for: {target_url}")
    print(f"  Prediction: {prediction_label}")
    print(f"  Confidence: {confidence:.2%}")
    print("--------------------------------------------------")


def main():
    """
    Main function to parse arguments, load the model, and process URLs.
    """
    # --- New: Set up command-line argument parsing ---
    parser = argparse.ArgumentParser(
        description="Classify one or more websites as Personal Blog or Corporate/SEO."
    )
    parser.add_argument(
        "urls", nargs='+', help="The website URL(s) to classify."
    )
    args = parser.parse_args()
    
    # --- Load Model Artifact (only once) ---
    print("--- Loading pre-trained model and vectorizer... ---")
    model_path = MODELS_DIR / "lgbm_final_model.joblib"
    try:
        artifact = joblib.load(model_path)
        model = artifact["model"]
        vectorizer = artifact["vectorizer"]
        print(f"✅ Model loaded successfully from: {model_path}")
    except Exception as e:
        print(f"❌ FATAL: Could not load model file at {model_path}. Error: {e}")
        sys.exit(1)

    # --- Loop through URLs from command line ---
    for url in args.urls:
        run_prediction_for_url(url, model, vectorizer)


if __name__ == "__main__":
    main()
