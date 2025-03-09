import json
import traceback
import requests
import fitz
import os
import csv
import re
import subprocess
import openai
import sys
import time
import logging
import random
from openai import OpenAI
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional

# Configure logging
logging.basicConfig(
    filename='pdf_utils.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

openai_key = os.environ.get("OPENAI_KEY")
if not openai_key:
    sys.exit("Environment variable OPENAI_KEY not set.")

openai.api_key = openai_key
client = OpenAI(api_key=openai.api_key)

def download_pdf(url: str, output_folder: str, csv_path: str, journal_name: str, article_link: str, cookies: dict) -> bool:
    """Download a PDF from a given URL and save it to the specified folder."""
    try:
        # Initialize a session with retry strategy
        session = requests.Session()
        retries = Retry(
            total=3,  # Total number of retries
            backoff_factor=1,  # Wait time between retries: {backoff factor} * (2 ^ ({number of total retries} - 1))
            status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
            allowed_methods=["GET"]  # Methods to retry on
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        # Define headers to mimic a real browser
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " \
                          "AppleWebKit/537.36 (KHTML, like Gecko) " \
                          "Chrome/58.0.3029.110 Safari/537.3",
            "Accept": "application/pdf",
            "Accept-Language": "en-US,en;q=0.9",
        }
        
        print(f"Attempting to download PDF: {url}")
        response = session.get(url, stream=True, timeout=30, headers=headers, cookies=cookies)
        print(f"Received response for {url}: Status Code {response.status_code}")
        
        if response.status_code == 200:
            content_type = response.headers.get('Content-Type', '')
            if 'application/pdf' not in content_type:
                print(f"Unexpected Content-Type for {url}: {content_type}. Skipping download.")
                return False
            
            os.makedirs(output_folder, exist_ok=True)
            filename = url.split('/')[-1].split('?')[0]  # Remove query parameters
            filepath = os.path.join(output_folder, filename)
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            print("*"*50)
            print(f'Downloaded {filename}')
            print("*"*50)

            # Convert the downloaded PDF to text
            convert_pdf_to_text(filepath, output_folder, csv_path)
            return True
        else:
            print(f'Error {response.status_code} while downloading {url}')
            return False
    except requests.exceptions.RequestException as e:
        print(f"RequestException occurred while downloading {url}: {e}")
        return False
    except Exception as e:
        print(f"Exception occurred while downloading {url}: {e}")
        traceback.print_exc()
        return False

def convert_pdf_to_text(pdf_path: str, output_folder:str, csv_path: str) -> Optional[str]:
    """Convert a PDF file to text and save it.
    
    Args:
        pdf_path (str): The path to the PDF file.
        output_folder (str): The folder to save the converted text file.
        csv_path (str): Path to the CSV file for logging.
    
    Returns:
        Optional[str]: The path to the converted text file, or None if conversion fails.
    """
    full_text = ""
    try:
        doc = fitz.open(pdf_path)
        for page_number in range(doc.page_count):
            page = doc[page_number]
            text = page.get_text("text")
            text = re.sub(r'\s+', ' ', text).strip()
            full_text += text + "\n"

        abstract_patterns = [r'\bAbstract\b', r'\bABSTRACT\b', r'\bSummary\b', r'\bExecutive Summary\b', r'\bIntroduction\b']
        references_patterns = [r'\bReferences\b', r'\bREFERENCES\b', r'\bBibliography\b', r'\bWorks Cited\b', r'\bLiterature Cited\b', r'\bReference List\b', r'\bCitations\b']
        abstract_start, references_start = None, None
        for pattern in abstract_patterns:
            match = re.search(pattern, full_text, re.IGNORECASE)
            if match:
                abstract_start = match.start()
                break
        for pattern in references_patterns:
            match = re.search(pattern, full_text, re.IGNORECASE)
            if match:
                references_start = match.start()
                break
        if abstract_start is not None and references_start is not None:
            full_text = full_text[abstract_start:references_start]
        else:
            print("Abstract or References section not found.")

        title = process_pdf(full_text, pdf_path, csv_path)
        
        new_name = f"{title}.txt"
        output_path = os.path.join(output_folder, new_name)
        with open(output_path, "w", encoding="utf-8") as file:
            file.write(full_text)
    except Exception as e:
        print(f"Error occurred while converting pdf to txt: {e}")
        traceback.print_exc()
        return None
    return output_path

def process_pdf(text: str, filename: str, csv_path:str) -> str:
    """
    Uses GPT Chat to attempt to extract structured information from a given text,
    explicitly requesting a JSON-formatted response. If extraction fails, it returns a default response.
    
    Args:
        text (str): The text to analyze and extract information from.
        
    Returns:
        str: The title of the paper if extracted successfully, otherwise an error message.
    """
    last_part = filename.split('/')[-1]
    doi = last_part.split('.')[0]
    
    with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['DOI'] == doi:
                print(f"DOI {doi} already exists in CSV.")
                return doi

    initial_prompt = f"""Given the following text, extract structured information in JSON format including the title, abstract, authors, keywords, institute of origin, DOI, and funding:
        
        Text: "{text}"
        
        Example Output:
        {{
            "Title": "Example Title",
            "Abstract": "Example abstract text",
            "Journal": "Example journal",
            "Relevant fields": ["field1", "field2"], 
            "Authors": ["Author One", "Author Two"],
            "Keywords": ["keyword1", "keyword2"],
            "Institute of Origin": "Example Institute",
            "DOI": "https://doi.org/example",
            "Funding": "Example funding source",
            "Methods": "Detailed information about the experimental setup with specific information about materials, techniques, formulas, numbers, metrics, etc. Write as much as you find.",
            "Results": "Detailed information about the experimental outcomes with specific information about materials, techniques, formulas, numbers, metrics, etc. Write as much as you find.",
            "Experiment details": "Detailed information about the experiment with specific information about materials, techniques, formulas, etc. Write as much as you find."
        }}

        Only output the JSON, so brackets and everything is inside. Nothing else.
        """

    messages = [
        {"role": "system", "content": "You are a helpful assistant, skilled in extracting structured information from research papers and outputting it in JSON format."},
        {"role": "user", "content": initial_prompt}
    ]

    try:
        completion = client.chat.completions.create(
            model="chatgpt-4o-latest",
            messages=messages,
            temperature=0.2
        )

        last_response = completion.choices[0].message.content

        if last_response.startswith("```json") and last_response.endswith("```"):
            last_response = last_response[len("```json"): -len("```")].strip()
        print("="*10)
        print("Model")
        print(completion.model)
        print("="*10)
        print("Token count")
        print(completion.usage.completion_tokens + int(completion.usage.prompt_tokens))
        print("="*10)
        print(last_response)

        try:
            parsed_response = json.loads(last_response)
        except json.JSONDecodeError as e:
            print(f"JSON decoding error: {e}")
            print("Raw response causing ERROR:", last_response)  # For debugging
            return "Error in JSON decoding"
        
        # Check if essential fields are present
        essential_fields = ['Title', 'Abstract', 'DOI']
        if not all(parsed_response.get(field) for field in essential_fields):
            print(f"Essential fields missing in response for DOI {doi}. Skipping entry.")
            return "Essential fields missing"
        
        print(parsed_response)
        json_output_path = "../data/rsc/doi/" + doi + ".json"
        
        with open(json_output_path, "w", encoding="utf-8") as json_file:
            json.dump(parsed_response, json_file, indent=4)

        authors = ', '.join(parsed_response['Authors'])
        keywords = ', '.join(parsed_response['Keywords'])
        relevant_fields = ', '.join(parsed_response.get('Relevant fields', []))  # Handles optional fields gracefully

        with open(csv_path, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'DOI', 'Title', 'Abstract', 'Journal', 'Relevant fields',
                'Authors', 'Keywords', 'Institute of Origin', 'Funding',
                'Methods', 'Results', 'Experiment details'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            data_to_write = {
                'DOI': parsed_response['DOI'],
                'Title': parsed_response['Title'],
                'Abstract': parsed_response['Abstract'],
                'Journal': parsed_response.get('Journal', 'N/A'),  # Using .get() for optional fields
                'Relevant fields': relevant_fields,
                'Authors': authors,
                'Keywords': keywords,
                'Institute of Origin': parsed_response['Institute of Origin'],
                'Funding': parsed_response['Funding'],
                'Methods': parsed_response.get('Methods', 'N/A'),
                'Results': parsed_response.get('Results', 'N/A'),
                'Experiment details': parsed_response.get('Experiment details', 'N/A')
            }

            writer.writerow(data_to_write)

    except json.JSONDecodeError as e:
        print(f"JSON decoding error: {e}")
        return "Error in JSON decoding"

    except Exception as e:
        print(f"An error occurred during PDF processing: {e}")
        traceback.print_exc()
        return "Error in processing PDF"

    print("Done with ", doi)
    print("*"*50)
    return doi