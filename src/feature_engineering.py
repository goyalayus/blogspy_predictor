# src/02_feature_engineering.py

import pandas as pd
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import re
import time
import logging

# Get a logger for this module
logger = logging.getLogger("FeatureEngineering")

def extract_url_features(urls: pd.Series) -> pd.DataFrame:
    """Extracts features from the URL string itself."""
    start_time = time.perf_counter()
    features = []
    for url in urls:
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc.replace('www.', '')
            path_parts = parsed_url.path.strip('/').split('/')

            # Common personal blog TLDs and subdomains
            is_dev_tld = 1 if domain.endswith(
                ('.dev', '.me', '.io', '.xyz', '.fyi', '.page')) else 0
            is_github_io = 1 if 'github.io' in domain else 0
            is_neocities = 1 if 'neocities.org' in domain else 0

            features.append({
                'url_len': len(url),
                'domain_len': len(domain),
                'path_depth': len(path_parts) if parsed_url.path != '/' else 0,
                'is_dev_tld': is_dev_tld,
                'is_github_io': is_github_io,
                'is_neocities': is_neocities,
            })
        except Exception:
            # Fallback for invalid URLs
            features.append({
                'url_len': 0, 'domain_len': 0, 'path_depth': 0,
                'is_dev_tld': 0, 'is_github_io': 0, 'is_neocities': 0,
            })
    
    duration_ms = (time.perf_counter() - start_time) * 1000
    logger.debug("URL feature extraction complete", extra={
        "event": {"name": "URL_FEATURE_EXTRACTION_COMPLETED", "duration_ms": round(duration_ms, 2)},
        "details": {"input": {"row_count": len(urls)}}
    })
    return pd.DataFrame(features)


def extract_structural_features(html_contents: pd.Series) -> pd.DataFrame:
    """Extracts features from the HTML structure and metadata."""
    start_time = time.perf_counter()
    features = []
    for html in html_contents:
        if not isinstance(html, str):
            # Handle cases where HTML fetching failed
            features.append({'generator_is_hugo': 0, 'generator_is_jekyll': 0, 'generator_is_wordpress': 0,
                             'has_hubspot_script': 0, 'link_count': 0, 'form_count': 0})
            continue
        try:
            soup = BeautifulSoup(html, 'html.parser')

            # Generator tags are a very strong signal
            generator_tag = soup.find('meta', attrs={'name': 'generator'})
            generator = generator_tag['content'].lower(
            ) if generator_tag else ''

            # Common marketing/tracking scripts
            scripts = [s.get('src', '')
                       for s in soup.find_all('script') if s.get('src')]
            has_hubspot = 1 if any(
                'js.hs-scripts.com' in s for s in scripts) else 0

            features.append({
                'generator_is_hugo': 1 if 'hugo' in generator else 0,
                'generator_is_jekyll': 1 if 'jekyll' in generator else 0,
                'generator_is_wordpress': 1 if 'wordpress' in generator else 0,
                'has_hubspot_script': has_hubspot,
                'link_count': len(soup.find_all('a')),
                'form_count': len(soup.find_all('form')),
            })
        except Exception:
            # Fallback for parsing errors
            features.append({'generator_is_hugo': 0, 'generator_is_jekyll': 0, 'generator_is_wordpress': 0,
                             'has_hubspot_script': 0, 'link_count': 0, 'form_count': 0})

    duration_ms = (time.perf_counter() - start_time) * 1000
    logger.debug("Structural feature extraction complete", extra={
        "event": {"name": "STRUCTURAL_FEATURE_EXTRACTION_COMPLETED", "duration_ms": round(duration_ms, 2)},
        "details": {"input": {"row_count": len(html_contents)}}
    })
    return pd.DataFrame(features)


def extract_content_features(text_contents: pd.Series) -> pd.DataFrame:
    """Extracts features from the visible text content."""
    start_time = time.perf_counter()
    features = []
    for text in text_contents:
        if not isinstance(text, str):
            # Handle cases where text could not be extracted
            features.append({'personal_pronoun_count': 0, 'corporate_pronoun_count': 0,
                             'corporate_keyword_count': 0})
            continue

        text_lower = text.lower()

        # Simple keyword/pronoun counting
        personal_pronouns = len(re.findall(r'\b(i|me|my)\b', text_lower))
        corporate_pronouns = len(re.findall(
            r'\b(we|our|company)\b', text_lower))
        corporate_keywords = len(re.findall(
            r'\b(solutions|enterprise|platform|services|b2b)\b', text_lower))

        features.append({
            'personal_pronoun_count': personal_pronouns,
            'corporate_pronoun_count': corporate_pronouns,
            'corporate_keyword_count': corporate_keywords,
        })
    
    duration_ms = (time.perf_counter() - start_time) * 1000
    logger.debug("Content feature extraction complete", extra={
        "event": {"name": "CONTENT_FEATURE_EXTRACTION_COMPLETED", "duration_ms": round(duration_ms, 2)},
        "details": {"input": {"row_count": len(text_contents)}}
    })
    return pd.DataFrame(features)
