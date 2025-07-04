# src/config.py

import pathlib

# --- Project Root ---
# This ensures paths work correctly when run from anywhere
ROOT_DIR = pathlib.Path(__file__).parent.parent

# --- Output/Model Directory ---
# The predict.py script uses this to find the .joblib file
OUTPUT_DIR = ROOT_DIR / "outputs"
MODELS_DIR = OUTPUT_DIR / "models"

# --- Label Mapping ---
# Used to translate the model's output (0 or 1) into a readable label
LABEL_MAPPING = {"corporate_seo": 0, "personal_blog": 1}
ID_TO_LABEL = {v: k for k, v in LABEL_MAPPING.items()}

# --- Web Request Parameters ---
# Used by predict.py when fetching website content
REQUEST_TIMEOUT = 10
REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
