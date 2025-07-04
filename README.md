
# BlogSpy Predictor

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10%2B-blue.svg" alt="Python version">
  <img src="https://img.shields.io/badge/License-MIT-green.svg" alt="License">
</p>

A command-line tool to classify websites as **Personal Blogs** or **Corporate/SEO** sites using a pre-trained LightGBM model. The classifier analyzes URL patterns, HTML structure, and text content to make its prediction.

This tool is the prediction-focused component of the full BlogSpy project, packaged for easy use as a standalone executable.

## Demo

Here is an example of `blogspy_predictor` in action:

```bash
# Analyze a personal blog and a corporate site
./blogspy_predictor --model main "https://gohugo.io/" "https://www.mongodb.com/what-is-mongodb"

# --- Output ---
# ... (loading and fetching logs)

--------------------------------------------------
✅ Results for: https://gohugo.io/
  Prediction: PERSONAL_BLOG
  Confidence: 99.87%
--------------------------------------------------

# ... (fetching logs for the next URL)

--------------------------------------------------
✅ Results for: https://www.mongodb.com/what-is-mongodb
  Prediction: CORPORATE_SEO
  Confidence: 99.98%
--------------------------------------------------
```

## Features

-   **High Accuracy:** Utilizes a powerful LightGBM model trained on a diverse dataset.
-   **Multi-faceted Analysis:** Goes beyond simple keywords by analyzing:
    -   **URL Features:** Domain name, path depth, and special TLDs (`.dev`, `.me`).
    -   **Structural Features:** HTML meta tags (e.g., `generator="hugo"`), link counts, and form presence.
    -   **Content Features:** Word and n-gram frequencies, and counts of personal vs. corporate language.
-   **Standalone CLI:** Packaged as a single executable with no external dependencies needed.
-   **Fast and Efficient:** Provides predictions for new URLs in seconds.

## Getting Started

You can use BlogSpy Predictor in two ways: by downloading the pre-built executable for your system, or by running it from the source code.

### Option 1: Use the Pre-built Executable (Recommended)

This is the easiest way to get started. No Python installation is required.

1.  Navigate to the [**Releases**](https://github.com/your-username/blogspy_predictor/releases) page of this repository.
2.  Download the `blogspy_predictor` executable for your operating system (e.g., Linux, macOS, or `blogspy_predictor.exe` for Windows).
3.  **On Linux/macOS:** You may need to make the file executable first.
    ```bash
    chmod +x ./blogspy_predictor
    ```
4.  Run the predictor from your terminal:
    ```bash
    ./blogspy_predictor --model main "https://some-website.com"
    ```

### Option 2: Run from Source Code (For Developers)

Use this method if you want to modify the code or run it in a custom environment.

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/blogspy_predictor.git
    cd blogspy_predictor
    ```

2.  **Create and activate a virtual environment:**
    ```bash
    # Create the environment
    python3 -m venv venv

    # Activate it (on Linux/macOS)
    source venv/bin/activate
    # On Windows: venv\Scripts\activate
    ```

3.  **Install the required dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Run the prediction script:**
    ```bash
    python src/predict.py --model main "https://some-website.com"
    ```

## How It Works

The prediction pipeline involves several steps to transform a raw URL into a classification:

1.  **Data Fetching:** The tool fetches the raw HTML content of the target URL using `requests`.
2.  **Content Parsing:** `BeautifulSoup` is used to parse the HTML. All `<script>` and `<style>` tags are removed, and the clean, human-readable text is extracted.
3.  **Feature Engineering:** Three distinct sets of features are generated:
    -   **URL Features:** `extract_url_features` analyzes the URL string itself for signals like length, path depth, and the presence of personal blog TLDs (`.dev`, `.me`, `github.io`).
    -   **Structural Features:** `extract_structural_features` inspects the HTML for strong indicators like `meta name="generator"` tags (which identify site builders like Hugo, Jekyll, or WordPress) and the number of links and forms.
    -   **Content Features:** `extract_content_features` performs a simple count of personal pronouns (`I`, `my`) vs. corporate language (`we`, `our`, `solutions`).
4.  **Text Vectorization:** The cleaned text content is converted into a high-dimensional numerical vector using a `HashingVectorizer`. This method is memory-efficient and captures word and bi-gram (two-word phrases) frequencies.
5.  **Prediction:** All engineered features (URL, structural, content, and text vectors) are concatenated into a single feature vector. This vector is then passed to the pre-trained `LightGBM` model, which outputs a probability score. A probability > 0.5 is classified as `PERSONAL_BLOG`, otherwise it is `CORPORATE_SEO`.

## Building the Executable Yourself

You can recreate the standalone binary using **PyInstaller**. This process bundles the Python interpreter, all necessary libraries, your source code, and the model file into one executable.

1.  **Ensure you have followed the "Run from Source" steps** to set up your environment and install dependencies.
2.  **Install PyInstaller:**
    ```bash
    pip install pyinstaller
    ```
3.  **Run the build command from the project root:**
    ```bash
    pyinstaller --onefile --name blogspy_predictor \
    --add-data "outputs/models/lgbm_final_model.joblib:outputs/models" \
    --hidden-import=sklearn.feature_extraction.text \
    --hidden-import=lightgbm \
    src/predict.py
    ```
    -   `--add-data`: This is crucial. It copies your model file into the executable, preserving its directory structure.
    -   `--hidden-import`: This tells PyInstaller to include libraries that are not explicitly imported in the source code but are required to unpickle the saved model objects (`HashingVectorizer` and the `LightGBM Booster`).

4.  Your finished executable will be located in the `dist/` directory.

## Project Structure

```
blogspy_predictor/
├── outputs/
│   └── models/
│       └── lgbm_final_model.joblib   # The pre-trained model artifact
├── src/
│   ├── __init__.py
│   ├── config.py                     # Configuration for paths and labels
│   ├── feature_engineering.py        # Functions to extract features
│   ├── predict.py                    # The main script for making predictions
│   └── utils.py                      # Utility functions (e.g., logger)
├── .gitignore
├── README.md                         # You are here!
└── requirements.txt                  # Python dependencies
```

## Contributing

Contributions are welcome! If you have ideas for new features or improvements, please open an issue to discuss it first. Pull requests are appreciated.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
