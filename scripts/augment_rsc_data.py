import requests
import pandas as pd
import argparse
import time
import logging
from tqdm import tqdm

def setup_logging():
    """Configure logging to display messages in the console."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def correct_doi(doi):
    """Corrects the DOI if the '10.1039/' prefix is missing."""
    if not doi.startswith('10.1039/'):
        return f'10.1039/{doi}'
    return doi

def fetch_metadata_crossref(doi):
    """Fetch metadata from CrossRef API for a given DOI."""
    url = f"https://api.crossref.org/works/{doi}"
    headers = {
        "User-Agent": "Metadata-Augmentation-Script/1.0"
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json().get('message', {})
        metadata = {
            'Title': data.get('title', [''])[0],
            'Abstract': data.get('abstract', ''),
            'Authors': ', '.join([' '.join(filter(None, [author.get('given'), author.get('family')])) for author in data.get('author', [])]),
            'Publication Year': data.get('published-print', {}).get('date-parts', [[None]])[0][0] or data.get('published-online', {}).get('date-parts', [[None]])[0][0],
            'Journal Name': data.get('container-title', [''])[0],
            'Volume': data.get('volume', ''),
            'Issue': data.get('issue', ''),
            'Pages': data.get('page', ''),
            'Publisher': data.get('publisher', ''),
            'Keywords': ', '.join(data.get('subject', [])),
            'Citation Count': data.get('is-referenced-by-count', 0),
            'Reference Count': data.get('reference-count', 0),
            'License': data.get('license', [{}])[0].get('URL', ''),
            'URL': data.get('URL', ''),
            'Language': data.get('language', ''),
            'DOI URL': f"https://doi.org/{doi}"
        }
        return metadata
    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception for DOI {doi}: {e}")
        return {}
    except Exception as e:
        logging.error(f"Error fetching metadata for DOI {doi}: {e}")
        return {}

def augment_dataset(input_file, output_file):
    """Augment the dataset with additional metadata."""
    # Read the existing dataset
    try:
        if input_file.endswith('.csv'):
            df = pd.read_csv(input_file)
        elif input_file.endswith('.json'):
            df = pd.read_json(input_file)
        else:
            logging.error("Unsupported file format. Use CSV or JSON.")
            return
    except Exception as e:
        logging.error(f"Error reading input file: {e}")
        return

    # Ensure 'DOI' column exists
    if 'DOI' not in df.columns:
        logging.error("The dataset must contain a 'DOI' column.")
        return

    # List of metadata fields to add
    metadata_fields = [
        'Title', 'Abstract', 'Authors', 'Publication Year', 'Journal Name',
        'Volume', 'Issue', 'Pages', 'Publisher', 'Keywords', 'Citation Count',
        'Reference Count', 'License', 'URL', 'Language', 'DOI URL'
    ]

    # Add metadata fields to the DataFrame if they don't exist
    for field in metadata_fields:
        if field not in df.columns:
            df[field] = ''

    # Fetch and augment metadata for each DOI
    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Augmenting dataset"):
        doi = correct_doi(row['DOI'])  # Correct the DOI if necessary
        metadata = fetch_metadata_crossref(doi)
        if metadata:
            for field in metadata_fields:
                df.at[index, field] = metadata.get(field, '')
        # Respect rate limits
        time.sleep(0.1)

    # Save the augmented dataset
    try:
        if output_file.endswith('.csv'):
            df.to_csv(output_file, index=False)
        elif output_file.endswith('.json'):
            df.to_json(output_file, orient='records', lines=True)
        logging.info(f"Successfully saved augmented dataset to {output_file}")
    except Exception as e:
        logging.error(f"Error saving output file: {e}")

def main():
    setup_logging()
    parser = argparse.ArgumentParser(description="Augment dataset with metadata from CrossRef API.")
    parser.add_argument(
        "-i", "--input_file",
        type=str,
        required=True,
        help="Path to the input dataset file (CSV or JSON)."
    )
    parser.add_argument(
        "-o", "--output_file",
        type=str,
        required=True,
        help="Path for the output augmented dataset file (CSV or JSON)."
    )
    args = parser.parse_args()

    augment_dataset(args.input_file, args.output_file)

if __name__ == "__main__":
    main()
