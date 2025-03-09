# pdf_utils.py

import json
import traceback
import requests
import os
import csv
import re
import logging
from typing import Optional

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set to INFO or DEBUG as needed

# Create console handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)  # Set to INFO or DEBUG as needed

# Create formatter and add to handler
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

# Add handler to logger
logger.addHandler(ch)

def download_pdf(url: str, output_folder: str, csv_path: str, doi: str) -> bool:
    """Download a PDF from a given URL and save it to the specified folder."""
    try:
        # Initialize a session with retry strategy
        session = requests.Session()
        retries = requests.adapters.Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = requests.adapters.HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        # Define headers to mimic a real browser
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/58.0.3029.110 Safari/537.3",
            "Accept": "application/pdf",
            "Accept-Language": "en-US,en;q=0.9",
        }
        
        logger.info(f"Attempting to download PDF: {url}")
        response = session.get(url, stream=True, timeout=30, headers=headers)
        logger.info(f"Received response for {url}: Status Code {response.status_code}")
        
        if response.status_code == 200:
            content_type = response.headers.get('Content-Type', '')
            if 'application/pdf' not in content_type:
                logger.warning(f"Unexpected Content-Type for {url}: {content_type}. Skipping download.")
                return False
            
            os.makedirs(output_folder, exist_ok=True)
            filename = f"{sanitize_filename(doi)}.pdf"  # Use sanitized DOI as filename
            filepath = os.path.join(output_folder, filename)
            
            if os.path.exists(filepath):
                logger.info(f"PDF already exists: {filepath}. Skipping download.")
                return True  # Already downloaded
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            logger.info(f"Downloaded {filename}")

            # Extract metadata and compile JSON
            extract_metadata(doi, csv_path)
            return True
        else:
            logger.error(f'Error {response.status_code} while downloading {url}')
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"RequestException occurred while downloading {url}: {e}")
        return False
    except Exception as e:
        logger.error(f"Exception occurred while downloading {url}: {e}")
        traceback.print_exc()
        return False

def extract_metadata(doi: str, csv_path: str) -> Optional[str]:
    """Fetch metadata from CrossRef API using DOI and append to CSV."""
    try:
        metadata = fetch_metadata_via_doi(doi)
        if not metadata:
            logger.warning(f"No metadata found via DOI: {doi}")
            metadata = {
                "DOI": doi,
                "Title": "N/A",
                "Abstract": "N/A",
                "Journal": "N/A",
                "Authors": "N/A",
                "Keywords": "N/A",
                "Institute of Origin": "N/A",
                "Funding": "N/A"
            }
        
        # Save JSON
        json_output_path = os.path.join("../data/rsc/doi", f"{sanitize_filename(doi)}.json")
        os.makedirs(os.path.dirname(json_output_path), exist_ok=True)
        with open(json_output_path, "w", encoding="utf-8") as json_file:
            json.dump(metadata, json_file, indent=4)
        logger.info(f"Saved JSON to {json_output_path}")
        
        # Append to CSV
        append_to_csv(metadata, csv_path)
        logger.info(f"Appended data for DOI {doi} to CSV.")
        
        return json_output_path
    
    except Exception as e:
        logger.error(f"Error occurred while extracting metadata for DOI {doi}: {e}")
        traceback.print_exc()
        return None

def fetch_metadata_via_doi(doi: str) -> Optional[dict]:
    """Fetch metadata from CrossRef API using DOI."""
    try:
        api_url = f"https://api.crossref.org/works/{doi}"
        response = requests.get(api_url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            item = data.get('message', {})
            metadata = {
                "DOI": doi,
                "Title": ' '.join(item.get('title', ['N/A'])),
                "Abstract": item.get('abstract', 'N/A'),
                "Journal": item.get('container-title', ['N/A'])[0],
                "Authors": ', '.join([f"{author.get('given', '')} {author.get('family', '')}".strip() for author in item.get('author', [])]),
                "Keywords": ', '.join(item.get('subject', [])),  # Using subjects as keywords
                "Institute of Origin": "N/A",  # Not available via CrossRef
                "Funding": "N/A"  # Funding info not typically available via CrossRef
            }
            return metadata
        else:
            logger.error(f"Failed to fetch metadata via DOI {doi}: Status Code {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Exception occurred while fetching metadata via DOI {doi}: {e}")
        traceback.print_exc()
        return None

def append_to_csv(data: dict, csv_path: str):
    """Append a single record to the CSV file."""
    try:
        with open(csv_path, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'DOI', 'Title', 'Abstract', 'Journal', 'Authors',
                'Keywords', 'Institute of Origin', 'Funding'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerow(data)
    except Exception as e:
        logger.error(f"Error occurred while appending data to CSV: {e}")
        traceback.print_exc()

def sanitize_filename(name: str) -> str:
    """Sanitize the filename by removing or replacing invalid characters."""
    return re.sub(r'[\\/*?:"<>|]', "_", name)
