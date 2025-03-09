# scrapers.py

import time
import logging
import traceback
from typing import Tuple, Optional
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from pdf_utils import download_pdf, sanitize_filename
from bs4 import BeautifulSoup
import re
import os
from tqdm import tqdm

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

def scrape_page_articles_rsc(
    url: str,
    output_folder: str,
    csv_path: str,
    journal_name: str,
    vpn_index: int,
) -> Tuple[Optional[str], int, int]:
    """Scrape articles from RSC's Themed Collections pages and download PDFs using Selenium."""
    count = 0

    # Set up Selenium WebDriver with headless Chrome using WebDriver Manager
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in headless mode
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/58.0.3029.110 Safari/537.3"
    )
    # Specify Chrome binary location if it's not in the default path
    chrome_options.binary_location = "/usr/bin/google-chrome"  # Update if necessary

    try:
        # Create a Service object
        service = Service(ChromeDriverManager().install())
        # Initialize WebDriver with service and options
        driver = webdriver.Chrome(service=service, options=chrome_options)
        logger.info("WebDriver initialized successfully.")
    except WebDriverException as e:
        logger.error(f"Error initializing WebDriver: {e}")
        return None, count, vpn_index

    driver.set_page_load_timeout(30)  # Set timeout

    try:
        driver.get(url)
        logger.info(f"Navigated to {url}")
    except TimeoutException:
        logger.error(f"Timeout while loading page: {url}")
        driver.quit()
        return None, count, vpn_index

    # Handle potential pop-ups (e.g., cookie consent)
    try:
        consent_button = driver.find_element(By.ID, "cookie-consent-accept")  # Update with actual ID
        consent_button.click()
        logger.info("Dismissed cookie consent.")
        time.sleep(2)
    except NoSuchElementException:
        logger.info("No cookie consent button found.")

    # Allow time for JavaScript to render
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div#tabthemedcollections"))
        )
    except TimeoutException:
        logger.error("Themed collections section did not load in time.")
        driver.quit()
        return None, count, vpn_index

    try:
        # Find all themed collection links
        themed_collection_elements = driver.find_elements(
            By.CSS_SELECTOR, "div#tabthemedcollections a.list__item-link"
        )
        themed_collection_links = [elem.get_attribute("href") for elem in themed_collection_elements]

        # Log each themed collection link and its text
        for idx, elem in enumerate(themed_collection_elements, start=1):
            logger.info(f"Collection {idx}: URL: {elem.get_attribute('href')}")
            logger.info(f"Collection {idx}: Text: {elem.text.strip()}")

        # Log number of themed collections found
        logger.info(f"Found {len(themed_collection_links)} themed collections.")

        if len(themed_collection_links) == 0:
            # Save page source for manual inspection
            with open("rsc_page_source_themed_collections.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            logger.warning("Saved page source to 'rsc_page_source_themed_collections.html' for debugging.")
    except NoSuchElementException:
        logger.error("No themed collection links found.")
        driver.quit()
        return None, count, vpn_index

    if not themed_collection_links:
        logger.info("No themed collections to process. Exiting scraper.")
        driver.quit()
        return None, count, vpn_index

    # Iterate over each themed collection link
    for collection_link in themed_collection_links:
        logger.info(f"Processing collection: {collection_link}")
        try:
            driver.get(collection_link)
            logger.info(f"Navigated to collection page: {collection_link}")
            # Capture screenshot for debugging
            driver.save_screenshot("current_collection_page_themed.png")
            logger.info("Saved screenshot of the collection page.")
        except TimeoutException:
            logger.error(f"Timeout while loading collection page: {collection_link}")
            continue

        # Allow time for JavaScript to render
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.article-list--content"))
            )
        except TimeoutException:
            logger.warning(f"Article list did not load in time for page: {collection_link}")
            continue

        while True:
            try:
                # Wait until articles are loaded
                WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.capsule"))
                )
            except TimeoutException:
                logger.warning(f"Timeout waiting for articles to load on page: {driver.current_url}")
                break

            # Extract page source and parse with BeautifulSoup
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'lxml')

            # Find all paper entries
            paper_entries = soup.find_all('div', class_='capsule')

            logger.info(f"Found {len(paper_entries)} papers on this page.")

            if not paper_entries:
                logger.info("No papers found on this page.")
                break

            # Prepare list of DOIs to process
            dois = []
            for entry in paper_entries:
                # Check if the article is open access by looking for "Edge Article"
                context_span = entry.find('span', class_='capsule__context')
                is_open_access = False
                if context_span and "Edge Article" in context_span.get_text():
                    is_open_access = True

                if not is_open_access:
                    logger.info("Skipping non-open access article.")
                    continue  # Skip non-open access articles

                # Extract DOI
                doi_tag = entry.find('a', href=re.compile(r'https?://doi\.org/'))
                doi = doi_tag['href'] if doi_tag else "N/A"

                if doi == "N/A":
                    logger.warning("No DOI found for an open access article. Skipping.")
                    continue

                dois.append(doi)

            if not dois:
                logger.info("No open access papers found on this page.")
                break

            # Remove already downloaded DOIs by checking if JSON file exists
            filtered_dois = []
            for doi in dois:
                doi_clean = doi.split('/')[-1].strip()  # Extract 'D4SC03066D' from 'https://doi.org/10.1039/D4SC03066D'
                json_path = os.path.join("../data/rsc/doi", f"{sanitize_filename(doi_clean)}.json")
                if not os.path.exists(json_path):
                    filtered_dois.append(doi_clean)
                else:
                    logger.info(f"Already downloaded DOI: {doi_clean}. Skipping.")

            if not filtered_dois:
                logger.info("All papers on this page have already been downloaded.")
                break

            # Initialize tqdm progress bar
            with tqdm(total=len(filtered_dois), desc="Downloading PDFs", unit="pdf") as pbar:
                # Process each paper sequentially
                for doi_clean in filtered_dois:
                    pdf_url = construct_pdf_url(doi_clean)
                    if not pdf_url:
                        logger.warning(f"Could not construct PDF URL for DOI: {doi_clean}. Skipping.")
                        pbar.update(1)
                        continue

                    success = download_pdf(pdf_url, output_folder, csv_path, doi_clean)
                    if success:
                        count += 1
                    pbar.update(1)
                    time.sleep(1)  # Optional delay to prevent rate limiting

            # Check for the "next page" button within the collection
            try:
                next_page_button = soup.find('a', class_='paging__btn paging__btn--next', attrs={'aria-disabled': 'false'})
                if next_page_button and 'href' in next_page_button.attrs:
                    next_page_href = next_page_button['href']
                    next_page_url = f"https://pubs.rsc.org{next_page_href}"
                    logger.info(f"Moving to next page: {next_page_url}")
                    driver.get(next_page_url)
                    # Capture screenshot for debugging
                    driver.save_screenshot("next_page_themed.png")
                    logger.info("Saved screenshot of the next page.")
                    time.sleep(5)  # Allow time for JavaScript to render
                else:
                    logger.info("No 'next page' button found. Ending pagination for this collection.")
                    break
            except Exception as e:
                logger.error(f"Exception occurred while navigating to next page: {e}")
                traceback.print_exc()
                break

    driver.quit()
    logger.info(f"Finished downloading articles from RSC Themed Collections. Total PDFs downloaded: {count}")
    return None, count, vpn_index

def scrape_issue_page_rsc(
    url: str,
    output_folder: str,
    csv_path: str,
    journal_name: str,
    vpn_index: int,
) -> Tuple[Optional[str], int, int]:
    """Scrape articles from RSC's Specific Issue pages and navigate through previous issues."""
    count = 0

    # Set up Selenium WebDriver with headless Chrome using WebDriver Manager
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in headless mode
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/58.0.3029.110 Safari/537.3"
    )
    # Specify Chrome binary location if it's not in the default path
    chrome_options.binary_location = "/usr/bin/google-chrome"  # Update if necessary

    try:
        # Create a Service object
        service = Service(ChromeDriverManager().install())
        # Initialize WebDriver with service and options
        driver = webdriver.Chrome(service=service, options=chrome_options)
        logger.info("WebDriver initialized successfully.")
    except WebDriverException as e:
        logger.error(f"Error initializing WebDriver: {e}")
        return None, count, vpn_index

    driver.set_page_load_timeout(30)  # Set timeout

    try:
        driver.get(url)
        logger.info(f"Navigated to {url}")
    except TimeoutException:
        logger.error(f"Timeout while loading page: {url}")
        driver.quit()
        return None, count, vpn_index

    # Handle potential pop-ups (e.g., cookie consent)
    try:
        consent_button = driver.find_element(By.ID, "cookie-consent-accept")  # Update with actual ID
        consent_button.click()
        logger.info("Dismissed cookie consent.")
        time.sleep(2)
    except NoSuchElementException:
        logger.info("No cookie consent button found.")

    # Allow time for JavaScript to render
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.article-list--content"))
        )
    except TimeoutException:
        logger.error("Article list section did not load in time.")
        driver.quit()
        return None, count, vpn_index

    while True:
        try:
            # Wait until articles are loaded
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.capsule"))
            )
        except TimeoutException:
            logger.warning(f"Timeout waiting for articles to load on page: {driver.current_url}")
            break

        # Extract page source and parse with BeautifulSoup
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'lxml')

        # Find all paper entries
        paper_entries = soup.find_all('div', class_='capsule')

        logger.info(f"Found {len(paper_entries)} papers on this page.")

        if not paper_entries:
            logger.info("No papers found on this page.")
            break

        # Prepare list of DOIs to process
        dois = []
        for entry in paper_entries:
            # Check if the article is open access by looking for "Edge Article"
            context_span = entry.find('span', class_='capsule__context')
            is_open_access = False
            if context_span and "Edge Article" in context_span.get_text():
                is_open_access = True

            if not is_open_access:
                logger.info("Skipping non-open access article.")
                continue  # Skip non-open access articles

            # Extract DOI
            doi_tag = entry.find('a', href=re.compile(r'https?://doi\.org/'))
            doi = doi_tag['href'] if doi_tag else "N/A"

            if doi == "N/A":
                logger.warning("No DOI found for an open access article. Skipping.")
                continue

            dois.append(doi)

        if not dois:
            logger.info("No open access papers found on this page.")
            break

        # Remove already downloaded DOIs by checking if JSON file exists
        filtered_dois = []
        for doi in dois:
            doi_clean = doi.split('/')[-1].strip()  # Extract 'D4SC03066D' from 'https://doi.org/10.1039/D4SC03066D'
            json_path = os.path.join("../data/rsc/doi", f"{sanitize_filename(doi_clean)}.json")
            if not os.path.exists(json_path):
                filtered_dois.append(doi_clean)
            else:
                logger.info(f"Already downloaded DOI: {doi_clean}. Skipping.")

        if not filtered_dois:
            logger.info("All papers on this page have already been downloaded.")
            break

        # Initialize tqdm progress bar
        with tqdm(total=len(filtered_dois), desc="Downloading PDFs", unit="pdf") as pbar:
            # Process each paper sequentially
            for doi_clean in filtered_dois:
                pdf_url = construct_pdf_url(doi_clean)
                if not pdf_url:
                    logger.warning(f"Could not construct PDF URL for DOI: {doi_clean}. Skipping.")
                    pbar.update(1)
                    continue

                success = download_pdf(pdf_url, output_folder, csv_path, doi_clean)
                if success:
                    count += 1
                pbar.update(1)
                time.sleep(1)  # Optional delay to prevent rate limiting

        # Check for the "Previous Issue" button and navigate if available
        try:
            prev_issue_button = soup.find('a', class_='paging__btn paging__btn--next', attrs={'aria-disabled': 'false'})
            if prev_issue_button and 'href' in prev_issue_button.attrs:
                next_page_href = prev_issue_button['href']
                next_page_url = f"https://pubs.rsc.org{next_page_href}"
                logger.info(f"Moving to next page: {next_page_url}")
                driver.get(next_page_url)
                # Capture screenshot for debugging
                driver.save_screenshot("next_page_themed.png")
                logger.info("Saved screenshot of the next page.")
                time.sleep(5)  # Allow time for JavaScript to render
            else:
                logger.info("No 'next page' button found. Ending pagination for this collection.")
                break
        except Exception as e:
            logger.error(f"Exception occurred while navigating to next page: {e}")
            traceback.print_exc()
            break

    driver.quit()
    logger.info(f"Finished downloading articles from RSC Themed Collections. Total PDFs downloaded: {count}")
    return None, count, vpn_index

def construct_pdf_url(doi_clean: str) -> Optional[str]:
    """Construct the correct PDF URL based on DOI."""
    # Parse DOI to extract year and article code
    # Example DOI: D4SC03066D
    match = re.match(r'D(\d)([A-Z0-9]+)', doi_clean)
    if not match:
        logger.warning(f"DOI format is unexpected: {doi_clean}. Cannot construct PDF URL.")
        return None

    year_digit = match.group(1)
    year = f"202{year_digit}"  # Correctly maps 'D4' to '2024'

    article_code_lower = doi_clean.lower()  # e.g., 'D4SC03066D' -> 'd4sc03066d'

    pdf_url = f"https://pubs.rsc.org/en/content/articlepdf/{year}/sc/{article_code_lower}"
    return pdf_url


def construct_pdf_url(doi_clean: str) -> Optional[str]:
    """Construct the correct PDF URL based on DOI."""
    # Parse DOI to extract year and article code
    # Example DOI: D4SC02981J
    match = re.match(r'D(\d)([A-Z0-9]+)', doi_clean)
    if not match:
        logger.warning(f"DOI format is unexpected: {doi_clean}. Cannot construct PDF URL.")
        return None

    year_digit = match.group(1)
    year = f"202{year_digit}"  # Correctly maps 'D4' to '2024'

    article_code_lower = doi_clean.lower()  # e.g., 'D4SC02981J' -> 'd4sc02981j'

    pdf_url = f"https://pubs.rsc.org/en/content/articlepdf/{year}/sc/{article_code_lower}"
    return pdf_url

def scrape_page_articles_acs(url: str, output_folder: str, csv_path:str, journal_name: str, vpn_index: int) -> Tuple[int, int]:
    """
    Scrape open access articles from an ACS, navigate to PDF page, and download PDFs.

    Args:
        url (str): The URL of the webpage.
        journal_name (str): The name of the journal for logging purposes.
        vpn_index (int): The index of the current VPN location.
        vpn_locations (List[str]): List of VPN locations.

    Returns:
        Tuple[int, int]: Updated count of downloaded PDFs and updated VPN index.
    """
    count = 0

    page = requests.get(url)
    if page.status_code == 200:
        soup = BeautifulSoup(page.content, 'html.parser')
        articles = soup.select('.issue-item_footer')

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for article in articles:
                open_access_img = article.find('img', alt="Open Access")
                if open_access_img:
                    pdf_link_element = article.find('a', title="PDF")
                    if pdf_link_element:
                        pdf_page_url = url + pdf_link_element['href']
                        pdf_page = requests.get(pdf_page_url)
                        if pdf_page.status_code == 200:
                            pdf_soup = BeautifulSoup(pdf_page.content, 'html.parser')
                            download_button = pdf_soup.find('a', class_='navbar-download')
                            if download_button and 'href' in download_button.attrs:
                                final_pdf_url = url + download_button['href']
                                futures.append(executor.submit(download_pdf, final_pdf_url, output_folder, csv_path, journal_name, final_pdf_url))

            for future in futures:
                if future.result():
                    count += 1
                    print(f"Downloaded {count} PDFs")

    else:
        print(f"Failed to retrieve the webpage. Status code: {page.status_code}")

    return count, vpn_index
    
def scrape_page_articles_nature(url: str, output_folder: str, csv_path:str, journal_name: str, base_url: str, vpn_index: int) -> Tuple[int, int]:
    """Scrape articles from Nature's website and download PDFs of open-access articles using concurrent futures for parallelization."""

    current_url = url
    count = 0

    while current_url:
        response = requests.get(current_url)
        if response.status_code != 200:
            print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
            break

        soup = BeautifulSoup(response.content, 'html.parser')
        article_items = soup.select('li.app-article-list-row__item')

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for article_item in article_items:
                if article_item.find('span', class_='u-color-open-access'):
                    pdf_link_element = article_item.find('a', {'data-article-pdf': 'true', 'data-test': 'download-pdf'})
                    if pdf_link_element:
                        pdf_url = url + pdf_link_element['href']
                        print(f"Found PDF link: {pdf_url}")
                        futures.append(executor.submit(download_pdf, pdf_url, output_folder, csv_path, journal_name, current_url))

            for future in futures:
                if future.result():
                    count += 1
                    print(f"Downloaded {count} PDFs")

        # Pagination
        next_page_link = soup.find('a', class_='c-pagination__link')
        current_url = url + next_page_link['href'] if next_page_link and 'href' in next_page_link.attrs else None

    print(f"Finished downloading. Total PDFs downloaded: {count}")
    return None, count, vpn_index

def scrape_page_articles_peerj(url: str, output_folder: str, csv_path: str, journal_name: str, vpn_index: int) -> Tuple[Optional[str], int, int]:
    """Scrape open access articles from PeerJ and download PDFs.

    Args:
        url (str): The URL of the webpage to start scraping from.
        journal_name (str): The name of the journal for logging and CSV updates.
        vpn_index (int): The index of the current VPN location.
        vpn_locations (List[str]): List of VPN locations.

    Returns:
        Tuple[Optional[str], int, int]: The URL of the next page (if any), updated count of downloaded PDFs, and updated VPN index.
    """
    count = 0

    page = requests.get(url)
    if page.status_code == 200:
        soup = BeautifulSoup(page.content, 'html.parser')
        articles = soup.select('div.main-search-item-row')

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for article in articles:
                article_link_element = article.find('a', href=True)
                if article_link_element:
                    article_url = f"{url.rsplit('/', 1)[0]}{article_link_element['href']}"
                    futures.append(executor.submit(download_pdf, article_url, output_folder, csv_path, journal_name, url))

            for future in futures:
                result = future.result()
                if result:
                    count += 1

        next_page_button = soup.find('button', {'aria-label': 'Next page'})
        if next_page_button:
            next_page_url = None
            return next_page_url, count, vpn_index
        else:
            return None, count, vpn_index
    else:
        print(f"Failed to retrieve the webpage. Status code: {page.status_code}")
        return None, count, vpn_index
    
def scrape_page_articles_aiche(url: str, output_folder: str, csv_path: str, journal_name: str, vpn_index: int) -> Tuple[Optional[str], int, int]:
    """Scrape articles from AICHE using concurrent futures for parallel downloads."""
    current_url = url
    count = 0
    while current_url:
        response = requests.get(current_url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            articles = soup.select('li.search__item')

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = []
                for article in articles:
                    open_access = article.find('div', class_='open-access')
                    if open_access:
                        title_link = article.find('a', href=True)
                        if title_link:
                            article_url = url + title_link['href']
                            pdf_link_element = article.find('a', class_='pdf-download', href=True)
                            if pdf_link_element:
                                pdf_url = url + pdf_link_element['href'].replace('/epdf/', '/pdfdirect/') + "?download=true"
                                futures.append(executor.submit(download_pdf, pdf_url, output_folder, csv_path, journal_name, url))

                for future in futures:
                    if future.result():
                        count += 1

            # Pagination
            next_page_link = soup.find('a', class_='pagination__next', href=True)
            current_url = url + next_page_link['href'] if next_page_link else None
        else:
            print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
            break

    return None, count, vpn_index

def scrape_page_articles_wiley(url: str, output_folder: str, csv_path: str, journal_name: str, vpn_index: int) -> Tuple[Optional[str], int, int]:
    """Scrape open access articles from a Wiley journal webpage and download PDFs using concurrent futures for parallelization."""
    count = 0
    while url:
        page = requests.get(url)
        if page.status_code == 200:
            soup = BeautifulSoup(page.content, 'html.parser')
            articles = soup.select('li.search__item')

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = []
                for article in articles:
                    pdf_link_element = article.find('a', href=True, text=re.compile("PDF"))
                    if pdf_link_element:
                        pdf_url = "https://chemistry-europe.onlinelibrary.wiley.com" + pdf_link_element['href']
                        futures.append(executor.submit(download_pdf, pdf_url, output_folder, csv_path, journal_name, url))

                for future in futures:
                    if future.result():
                        count += 1

            # Pagination
            next_page_link = soup.find('a', class_='pagination__btn--next', href=True)
            url = next_page_link['href'] if next_page_link else None
        else:
            print(f"Failed to retrieve the webpage. Status code: {page.status_code}")
            break

    return None, count, vpn_index