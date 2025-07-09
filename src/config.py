# src/config.py

import pathlib

# --- Project Root ---
ROOT_DIR = pathlib.Path(__file__).parent.parent

# --- Log Directory ---
LOGS_DIR = ROOT_DIR / "logs"

# --- Output/Model Directory ---
OUTPUT_DIR = ROOT_DIR / "outputs"
MODELS_DIR = OUTPUT_DIR / "models"

# --- Web Request Parameters ---
REQUEST_TIMEOUT = 10
REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
