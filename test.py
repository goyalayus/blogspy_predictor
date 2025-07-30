# verify_prediction.py

import sys
import pathlib
import requests
import joblib
import pandas as pd
import scipy.sparse as sp
from bs4 import BeautifulSoup

# --- Setup Project Paths to Import Your Modules ---
# This ensures the script can find your 'src' folder and its files.
project_root = pathlib.Path(__file__).parent
src_path = project_root / "src"
sys.path.append(str(project_root.resolve()))

from src.config import MODELS_DIR, REQUEST_HEADERS
from src.feature_engineering import (
    extract_url_features,
    extract_structural_features,
    extract_content_features,
)

# --- The URL to Test ---
TARGET_URL = "https://goyalayus.github.io/"

def main():
    """
    Runs the full fetch, feature engineering, and prediction pipeline
    for a single URL to verify the model's output.
    """
    print(f"--- Verifying prediction for: {TARGET_URL} ---")

    # 1. FETCH CONTENT (Mimicking the Go Worker)
    print("\n[Step 1/5] Fetching web content...")
    try:
        response = requests.get(TARGET_URL, headers=REQUEST_HEADERS, timeout=10)
        response.raise_for_status()
        html_content = response.text
        print(f"✅ Successfully fetched {len(html_content)} bytes of HTML.")
    except requests.RequestException as e:
        print(f"❌ Failed to fetch URL: {e}")
        return

    # 2. PARSE & CLEAN CONTENT (Mimicking the Go Worker)
    print("\n[Step 2/5] Parsing and cleaning HTML content...")
    soup = BeautifulSoup(html_content, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    
    # Clean up whitespace similar to how the Go worker would
    text_content = " ".join(soup.get_text().split())
    print(f"✅ Extracted {len(text_content)} characters of clean text.")

    # 3. LOAD MODEL ARTIFACT
    print("\n[Step 3/5] Loading pre-trained model and vectorizer...")
    model_path = MODELS_DIR / "lgbm_final_model.joblib"
    try:
        artifact = joblib.load(model_path)
        model = artifact["model"]
        vectorizer = artifact["vectorizer"]
        print(f"✅ Model artifact loaded successfully from: {model_path}")
    except FileNotFoundError:
        print(f"❌ FATAL: Model file not found at {model_path}")
        return
    except KeyError:
        print("❌ FATAL: Model artifact is invalid. Missing 'model' or 'vectorizer' key.")
        return

    # 4. RUN PREDICTION PIPELINE (Mimicking the Python API)
    print("\n[Step 4/5] Running feature engineering and prediction...")
    
    # Create a single-row DataFrame to match the input format of our functions
    data = pd.DataFrame([{
        "url": TARGET_URL,
        "html_content": html_content,
        "text_content": text_content,
    }])

    # Text Vectorization
    txt_feat = vectorizer.transform(data["text_content"])

    # Feature Engineering
    url_feats = extract_url_features(data["url"])
    struct_feats = extract_structural_features(data["html_content"])
    content_feats = extract_content_features(data["text_content"])

    # Combine all features in the correct order
    num_feat_df = pd.concat([url_feats, struct_feats, content_feats], axis=1)
    num_feat = sp.csr_matrix(num_feat_df.to_numpy(dtype="float32"))
    features = sp.hstack([txt_feat, num_feat], format="csr")

    print(f"✅ Combined feature vector created with shape: {features.shape}")

    # Make the prediction
    prediction_prob = model.predict(features)[0]

    # 5. DISPLAY RESULTS
    print("\n[Step 5/5] Final Results:")
    print("--------------------------------------------------")
    
    is_personal_blog = prediction_prob > 0.5
    prediction_label = "PERSONAL_BLOG" if is_personal_blog else "CORPORATE_SEO"
    
    # Calculate confidence score
    confidence = prediction_prob if is_personal_blog else 1 - prediction_prob

    print(f"✅ URL: {TARGET_URL}")
    print(f"  - Raw Probability Score: {prediction_prob:.4f}")
    print(f"  - Prediction: {prediction_label}")
    print(f"  - Confidence: {confidence:.2%}")
    print("--------------------------------------------------")


if __name__ == "__main__":
    main()
