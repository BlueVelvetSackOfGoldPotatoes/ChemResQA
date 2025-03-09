import os
import json
import pandas as pd
import argparse
from typing import List, Dict
import logging
import requests
from tqdm import tqdm
import time
import re
import csv

def setup_logging():
    """Configure logging to display messages in the console."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def sanitize_text(text: str) -> str:
    """
    Sanitize text by removing or replacing unwanted characters.

    Args:
        text (str): The text to sanitize.

    Returns:
        str: The sanitized text.
    """
    if isinstance(text, str):
        # Replace tabs, newlines, and carriage returns with spaces
        text = re.sub(r'[\t\n\r]+', ' ', text)
        # Optionally, remove HTML tags if desired
        text = re.sub(r'<[^>]+>', '', text)
        # Strip leading and trailing whitespace
        text = text.strip()
        return text
    return text

def read_json_files(input_dir: str) -> List[Dict]:
    """
    Read all JSON files in the specified directory and extract DOI entries.

    Args:
        input_dir (str): Path to the directory containing JSON files.

    Returns:
        List[Dict]: A list of dictionaries containing DOI entries.
    """
    doi_entries = []
    json_files = [file for file in os.listdir(input_dir) if file.endswith('.json')]

    if not json_files:
        logging.warning(f"No JSON files found in directory: {input_dir}")
        return doi_entries

    for file in json_files:
        file_path = os.path.join(input_dir, file)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Ensure 'DOI' field exists
                if 'DOI' in data:
                    doi_original = data['DOI'].strip()
                    # Add prefix if missing
                    if not doi_original.lower().startswith("10.1039/"):
                        # Attempt to extract DOI from filename if possible
                        filename = os.path.splitext(file)[0]
                        # Check if filename starts with "10.1039/"
                        if re.match(r'^10\.1039/.+', filename, re.IGNORECASE):
                            doi_full = filename
                            logging.info(f"Using filename to correct DOI for {file}: {doi_full}")
                        else:
                            doi_full = f"10.1039/{doi_original}"
                            logging.info(f"Added prefix to DOI for {file}: {doi_full}")
                        data['DOI'] = doi_full
                    else:
                        logging.info(f"DOI is complete for {file}: {doi_original}")
                    # Sanitize all text fields
                    for key in data:
                        data[key] = sanitize_text(data[key])
                    doi_entries.append(data)
                else:
                    logging.warning(f"File {file} does not contain 'DOI'. Skipping.")
        except json.JSONDecodeError:
            logging.error(f"File {file} is not a valid JSON. Skipping.")
        except Exception as e:
            logging.error(f"An error occurred while reading {file}: {e}. Skipping.")

    return doi_entries

def fetch_metadata_openalex(doi: str) -> Dict:
    """
    Fetch metadata for a given DOI from the OpenAlex API.

    Args:
        doi (str): The complete DOI of the publication (including prefix).

    Returns:
        Dict: A dictionary containing fetched metadata or empty dict if failed.
    """
    # OpenAlex expects the DOI to be URL-encoded without special characters
    doi_url = f"https://doi.org/{doi}"
    api_url = f"https://api.openalex.org/works/{requests.utils.quote(doi_url, safe='')}"
    headers = {
        "User-Agent": "DOI-Enrichment-Script/1.0"
    }
    try:
        response = requests.get(api_url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            # Extract relevant fields
            metadata = {
                "Title": data.get("title", "N/A"),
                "Abstract": data.get("abstract", "N/A"),  # OpenAlex provides 'abstract'
                "Journal": data.get("host_venue", {}).get("display_name", "N/A"),
                "Authors": ', '.join([author.get("author", {}).get("display_name", "") for author in data.get("authorships", []) if author.get("author")]),
                "Keywords": ', '.join([keyword.get("display_name", "") for keyword in data.get("authorships", []) if keyword.get("display_name")]) if data.get("authorships") else "N/A",
                "Institute of Origin": ', '.join([affiliation.get("institution", {}).get("display_name", "") for affiliation in data.get("authorships", []) if affiliation.get("institution")]) if data.get("authorships") else "N/A",
                "Funding": "N/A"  # OpenAlex does not provide funding info directly
            }
            return metadata
        else:
            logging.warning(f"OpenAlex API returned status code {response.status_code} for DOI: {doi}")
            return {}
    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception for DOI {doi}: {e}")
        return {}
    except Exception as e:
        logging.error(f"Unexpected error for DOI {doi}: {e}")
        return {}

def merge_and_enrich_dois(doi_entries: List[Dict], output_file: str) -> pd.DataFrame:
    """
    Merge DOI entries, enrich missing metadata, and save to CSV.

    Args:
        doi_entries (List[Dict]): List of DOI entries.
        output_file (str): Path for the output CSV file.

    Returns:
        pd.DataFrame: The merged and enriched DataFrame.
    """
    # Convert list of dicts to DataFrame
    df = pd.DataFrame(doi_entries)

    # List of expected columns
    expected_columns = ["DOI", "Title", "Abstract", "Journal", "Authors", "Keywords", "Institute of Origin", "Funding"]

    # Ensure all expected columns are present
    for col in expected_columns:
        if col not in df.columns:
            df[col] = "N/A"

    # Initialize Enrichment Status column
    df["Enrichment Status"] = "Not Attempted"

    # Iterate over each row and enrich data if needed
    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Enriching DOIs"):
        if row["Enrichment Status"] == "Success":
            continue  # Skip already enriched
        doi = row["DOI"]
        # Check if essential fields are missing
        missing_fields = [field for field in ["Title", "Abstract", "Journal", "Authors"] if row[field] == "N/A"]
        if not missing_fields:
            df.at[index, "Enrichment Status"] = "Already Complete"
            continue  # No need to enrich

        # Fetch metadata from OpenAlex
        metadata = fetch_metadata_openalex(doi)
        if metadata:
            # Update missing fields
            for field in ["Title", "Abstract", "Journal", "Authors", "Keywords", "Institute of Origin", "Funding"]:
                if row[field] == "N/A" and metadata.get(field, "N/A") != "N/A":
                    df.at[index, field] = metadata[field]
            df.at[index, "Enrichment Status"] = "Success"
        else:
            df.at[index, "Enrichment Status"] = "Failed"

        # To respect API rate limits
        time.sleep(0.1)  # Adjust sleep time as needed

    # Drop duplicates based on DOI, keeping the first occurrence
    initial_count = len(df)
    df.drop_duplicates(subset="DOI", keep='first', inplace=True)
    final_count = len(df)
    if initial_count != final_count:
        logging.info(f"Dropped {initial_count - final_count} duplicate DOIs.")

    # Sanitize all text fields to remove unwanted characters
    for col in expected_columns + ["Enrichment Status"]:
        df[col] = df[col].apply(sanitize_text)

    # Save to CSV with proper quoting
    try:
        df.to_csv(output_file, index=False, encoding='utf-8', quoting=csv.QUOTE_ALL)
        logging.info(f"Successfully saved merged and enriched CSV to {output_file}")
    except Exception as e:
        logging.error(f"Failed to save CSV: {e}")

    return df

def main():
    setup_logging()

    parser = argparse.ArgumentParser(description="Merge JSON DOI files into a CSV and enrich missing metadata.")
    parser.add_argument(
        "-i", "--input_dir",
        type=str,
        required=True,
        help="Path to the directory containing JSON DOI files."
    )
    parser.add_argument(
        "-o", "--output_file",
        type=str,
        required=True,
        help="Path for the output merged CSV file."
    )

    args = parser.parse_args()

    input_dir = args.input_dir
    output_file = args.output_file

    # Check if input directory exists
    if not os.path.isdir(input_dir):
        logging.error(f"The input directory '{input_dir}' does not exist.")
        return

    # Read JSON files
    doi_entries = read_json_files(input_dir)

    if not doi_entries:
        logging.warning("No DOI entries found. Exiting.")
        return

    # Merge and enrich
    merged_df = merge_and_enrich_dois(doi_entries, output_file)

    # Optional: Save a log of DOIs that failed enrichment
    failed_enrichment = merged_df[merged_df["Enrichment Status"] == "Failed"]
    if not failed_enrichment.empty:
        failed_dois = failed_enrichment["DOI"].tolist()
        with open("failed_enrichments.txt", "w") as f:
            for doi in failed_dois:
                f.write(f"{doi}\n")
        logging.info(f"Saved list of failed DOIs to 'failed_enrichments.txt'.")

if __name__ == "__main__":
    main()
