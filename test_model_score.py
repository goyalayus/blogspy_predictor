import sys
import pathlib
import joblib
import pandas as pd
import requests
from bs4 import BeautifulSoup
import scipy.sparse as sp
import sklearn, lightgbm, platform, hashlib

# --- Configuration ---
TARGET_URL = "https://habr.com/"
MODEL_PATH = "outputs/models/lgbm_final_model.joblib"
REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def main():
    print("VERSIONS", {
        "python": sys.version,
        "platform": platform.platform(),
        "sklearn": sklearn.__version__,
        "lightgbm": lightgbm.__version__,
    })

    project_root = pathlib.Path(__file__).parent
    sys.path.append(str(project_root.resolve()))
    from src.feature_engineering import (
        extract_url_features,
        extract_structural_features,
        extract_content_features,
    )

    print(f"Loading model artifact from: {MODEL_PATH}")
    try:
        model_artifact = joblib.load(MODEL_PATH)
        vectorizer = model_artifact["vectorizer"]
        model = model_artifact["model"]
        print("✅ Model loaded successfully.")
    except FileNotFoundError:
        print(f"❌ ERROR: Model file not found at '{MODEL_PATH}'.")
        return

    print(f"\nFetching content for: {TARGET_URL}")
    try:
        response = requests.get(TARGET_URL, headers=REQUEST_HEADERS, timeout=15)
        response.raise_for_status()
        html_content = response.text
        print("✅ Content fetched successfully.")
    except requests.exceptions.RequestException as e:
        print(f"❌ ERROR: Failed to fetch URL: {e}")
        return

    print("Parsing HTML and extracting text...")
    soup = BeautifulSoup(html_content, 'lxml')
    for script_or_style in soup(["script", "style"]):
        script_or_style.decompose()
    text_content = " ".join(soup.stripped_strings)
    print("✅ Text extracted.")

    # Input fingerprints
    print("INPUT FINGERPRINTS (local)", {
        "html_md5": hashlib.md5(html_content.encode()).hexdigest(),
        "text_md5": hashlib.md5(text_content.encode()).hexdigest(),
        "text_len": len(text_content),
        "html_len": len(html_content),
    })

    data = pd.DataFrame([{
        "url": TARGET_URL,
        "html_content": html_content,
        "text_content": text_content,
    }])

    print("\nGenerating features...")
    txt_feat = vectorizer.transform(data["text_content"])
    url_feats = extract_url_features(data["url"])
    struct_feats = extract_structural_features(data["html_content"])
    content_feats = extract_content_features(data["text_content"])
    print("✅ All features generated.")

    # Feature fingerprints
    print("FEATURE FINGERPRINTS (local)", {
        "txt_nnz": int(txt_feat.nnz),
        "txt_sum": float(txt_feat.sum()),
        "txt_shape": tuple(txt_feat.shape),
    })

    num_feat_df = pd.concat([url_feats, struct_feats, content_feats], axis=1)
    num_feat = sp.csr_matrix(num_feat_df.to_numpy(dtype="float32"))
    final_features = sp.hstack([txt_feat, num_feat], format="csr")
    print(f"✅ Final feature vector created with shape: {final_features.shape}")

    print("FEATURE FINGERPRINTS (local, final)", {
        "num_feat_sum": float(num_feat.sum()),
        "final_nnz": int(final_features.nnz),
        "final_shape": tuple(final_features.shape),
    })

    print("\nMaking prediction...")
    prediction_prob = model.predict(final_features)[0]

    print("\n--------------------------------------------------")
    print(f"🔬 Results for: {TARGET_URL}")
    print("--------------------------------------------------")
    print(f"Raw Probability Score: {prediction_prob:.4f}")
    print(
        "(Score closer to 1.0 means more likely 'Personal Blog'; "
        "closer to 0.0 means 'Corporate/SEO')"
    )
    is_personal_blog = bool(prediction_prob > 0.75)
    print(f"\nFinal Classification (at >0.75 threshold): {'PERSONAL_BLOG' if is_personal_blog else 'CORPORATE_SEO'}")
    print("--------------------------------------------------")

if __name__ == "__main__":
    main()
