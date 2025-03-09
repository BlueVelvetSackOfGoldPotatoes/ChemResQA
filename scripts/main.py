# main.py

import traceback
import os
import csv
from publishers_links import publishers_links
from scrapers import scrape_page_articles_rsc, scrape_issue_page_rsc

OUTPUT_FOLDER: str = "../data/rsc/pdfs"
CSV_PATH: str = "../data/rsc_downloaded_articles.csv"

def main():
    """Main function to initiate scraping and downloading of open access papers."""

    # Create necessary directories
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    os.makedirs("../data/rsc/doi", exist_ok=True)  # Directory for JSON files

    # Define CSV fieldnames
    fieldnames = [
        'DOI', 'Title', 'Abstract', 'Journal', 'Authors',
        'Keywords', 'Institute of Origin', 'Funding'
    ]

    # Initialize CSV if it doesn't exist
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

    vpn_index = 0
    for publisher_key, links in publishers_links.items():
        journal_name = publisher_key.replace("_links", "").lower()  # Ensure lowercase
        print(f"Starting scraping for {journal_name}")

        # Determine scraper function based on the starting URL pattern
        for link in links:
            print(f"Scraping {link}")
            try:
                if "themedcollections" in link:
                    # Scraper for Themed Collections
                    _, temp_count, vpn_index = scrape_page_articles_rsc(link, OUTPUT_FOLDER, CSV_PATH, journal_name, vpn_index)
                elif "issueid=" in link:
                    # Scraper for Specific Issues
                    _, temp_count, vpn_index = scrape_issue_page_rsc(link, OUTPUT_FOLDER, CSV_PATH, journal_name, vpn_index)
                else:
                    print(f"Unknown link format: {link}. Skipping.")
                    continue
                print(f"Downloaded {temp_count} PDFs from {link}")
            except Exception as e:
                print(f"Error occurred while scraping {link}: {e}")
                traceback.print_exc()
            print("="*50)  # Divider after each link
        print("="*100)  # Divider after each publisher

if __name__ == '__main__':
    main()
