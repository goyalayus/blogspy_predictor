import sys
import pathlib
import joblib
import pandas as pd
import requests
from bs4 import BeautifulSoup
import scipy.sparse as sp

# --- Configuration ---
# The URL of the homepage you want to test
TARGET_URL = "https://squarespace.com"

# Path to your trained model file
MODEL_PATH = "outputs/models/lgbm_final_model.joblib"

# Use the same headers as the crawler to avoid being blocked
REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# --- Main Script ---

def main():
    """
    Loads the model, fetches a URL, runs the full prediction pipeline,
    and prints the raw probability score.
    """
    # Add the 'src' directory to the Python path to import our functions
    project_root = pathlib.Path(__file__).parent
    sys.path.append(str(project_root.resolve()))
    
    from src.feature_engineering import (
        extract_url_features,
        extract_structural_features,
        extract_content_features,
    )

    # 1. Load the pre-trained model and vectorizer
    print(f"Loading model artifact from: {MODEL_PATH}")
    try:
        model_artifact = joblib.load(MODEL_PATH)
        vectorizer = model_artifact["vectorizer"]
        model = model_artifact["model"]
        print("✅ Model loaded successfully.")
    except FileNotFoundError:
        print(f"❌ ERROR: Model file not found at '{MODEL_PATH}'.")
        print("Please ensure the file exists and you are running the script from the project root.")
        return

    # 2. Fetch the content of the target URL
    print(f"\nFetching content for: {TARGET_URL}")
    try:
        response = requests.get(TARGET_URL, headers=REQUEST_HEADERS, timeout=15)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        html_content = response.text
        print("✅ Content fetched successfully.")
    except requests.exceptions.RequestException as e:
        print(f"❌ ERROR: Failed to fetch URL: {e}")
        return

    # 3. Parse content with BeautifulSoup to get clean text
    print("Parsing HTML and extracting text...")
    soup = BeautifulSoup(html_content, 'lxml')
    for script_or_style in soup(["script", "style"]):
        script_or_style.decompose()
    text_content = " ".join(soup.stripped_strings)
    print("✅ Text extracted.")

    # 4. Create a DataFrame, as expected by the feature engineering functions
    data = pd.DataFrame([{
        "url": TARGET_URL,
        "html_content": html_content,
        "text_content": text_content,
    }])

    # 5. Generate all feature sets
    print("\nGenerating features...")
    # a. Text features (vectorization)
    txt_feat = vectorizer.transform(data["text_content"])
    # b. URL features
    url_feats = extract_url_features(data["url"])
    # c. Structural features from HTML
    struct_feats = extract_structural_features(data["html_content"])
    # d. Content features from clean text
    content_feats = extract_content_features(data["text_content"])
    print("✅ All features generated.")

    # 6. Combine all features into a single sparse matrix for the model
    print("Combining features for prediction...")
    num_feat_df = pd.concat([url_feats, struct_feats, content_feats], axis=1)
    num_feat = sp.csr_matrix(num_feat_df.to_numpy(dtype="float32"))
    final_features = sp.hstack([txt_feat, num_feat], format="csr")
    print(f"✅ Final feature vector created with shape: {final_features.shape}")

    # 7. Make the prediction
    print("\nMaking prediction...")
    prediction_prob = model.predict(final_features)[0]

    # 8. Display the results
    print("\n--------------------------------------------------")
    print(f"🔬 Results for: {TARGET_URL}")
    print("--------------------------------------------------")
    print(f"Raw Probability Score: {prediction_prob:.4f}")
    print(
        "(Score closer to 1.0 means more likely 'Personal Blog'; "
        "closer to 0.0 means 'Corporate/SEO')"
    )

    # Use the same threshold as your API to give a final classification
    is_personal_blog = bool(prediction_prob > 0.75)
    print(f"\nFinal Classification (at >0.75 threshold): {'PERSONAL_BLOG' if is_personal_blog else 'CORPORATE_SEO'}")
    print("--------------------------------------------------")

if __name__ == "__main__":
    main()
