# File: gateio_get_articles_refactor.py

import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
import time
import logging
import logger_setup

ARTICLE_COLLECTION_FILE = os.path.expanduser('~/parsley/Gateio_Files/Gateio_Article_Process/gateio_article_collection.tsv')

# Function to get HTML content with retry mechanism
def get_html(url, max_retries=3, backoff_factor=2):
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.text
        except requests.exceptions.HTTPError as e:
            if response.status_code == 502:
                retries += 1
                wait_time = backoff_factor ** retries
                logging.error(f"502 Server Error: Bad Gateway for url: {url}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logging.error(f"HTTP Error fetching {url}: {e}")
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching {url}: {e}")
            return None
    logging.error(f"Failed to fetch {url} after {max_retries} retries.")
    return None

# Function to clean main_content (body) content using regex
def clean_body(main_content):
    
    main_content = re.sub(r'\[([^\]]+)\]\(\s*(?:[^\s\)]+)(?:\s+"[^"]*")?\s*\)', r'\1', main_content) # Remove markdown links
    main_content = re.sub(r'!\[.*?\]\(.*?\)', '', main_content)  # Remove markdown images
   
    main_content = re.sub(r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F700-\U0001F77F\u2700-\u27BF\u2600-\u26FF\uFE0F]', '', main_content)
    main_content = re.sub(r'\u00A0', ' ', main_content)
    main_content = re.sub(r'[\u25CB-\u25EF\u2B55\u1F9E7\u2B50]', '', main_content)

    main_content = re.sub(r'\uff1a', ': ', main_content)
    main_content = re.sub(r'\uff01', '! ', main_content)
    main_content = re.sub(r'\.\.', '.', main_content)
    main_content = re.sub(r' ,', ',', main_content)
    main_content = re.sub(r' :', ':', main_content)

    main_content = re.sub(r'\u2013', '-', main_content)  # Replaces en dash with a hyphen
    main_content = re.sub(r'["\u201c\u201d\u2018\u2019]', '', main_content)
    main_content = re.sub(r'\t+', ' ', main_content)
    main_content = re.sub(r'&', 'and', main_content)

    main_content = re.sub(r'\u3010.*?\u3011', '', main_content)  # Remove text within 【 and 】
    main_content = re.sub(r'\uff06', 'and', main_content)
    main_content = re.sub(r'\uff08', '(', main_content)
    main_content = re.sub(r'\uff09', ')', main_content)
    main_content = re.sub(r'\u25cf', '\u2022', main_content)

    main_content = re.sub(r'\n\s+', '\n', main_content)
    main_content = re.sub(r'\n{2,}', '\n', main_content)

    main_content = re.sub(r'\[//\]:content-type-MARKDOWN-DONOT-DELETE\s*\n?', '', main_content)

    main_content = re.sub(r'\s*Gateway to Crypto.*', '', main_content, flags=re.DOTALL).rstrip()
    main_content = re.sub(r'\s*Gate.io is your gateway to crypto.*', '', main_content, flags=re.DOTALL).rstrip()
    main_content = re.sub(r'\s*Gate.io is a Cryptocurrency Trading Platform Since 2013.*', '', main_content, flags=re.DOTALL).rstrip()
    main_content = re.sub(r'\s*The gateway to cryptocurrency.*', '', main_content, flags=re.DOTALL).rstrip()

    main_content = re.sub(r'(?:\r\n|\r|\n|\u2028|\u2029)+', '///', main_content)

    return main_content #body

# Function to parse article HTML content
def parse_article_html(html):
    soup = BeautifulSoup(html, 'html.parser')

    # Extract publish time
    article_details_box = soup.find('div', class_='article-details-box')
    if not article_details_box:
        logging.error(f"Article details box class not found in HTML")
        return None, None
    
    publish_datetime = article_details_box.find('div', class_='article-details-base-info').find_all('span')[0].get_text(strip=True)

    # Extract the main content within the article-details-main
    main_content_div = article_details_box.find('div', class_='article-details-main')
    main_content = main_content_div.get_text(strip=True, separator='\n') if main_content_div else ''

    # Clean the content using the new clean_body function
    main_content = clean_body(main_content)
    
    return main_content, publish_datetime

# Function to process articles from gateio_article_list.tsv
def get_articles():
    
    # Load the article list
    article_list_df = pd.read_csv(ARTICLE_COLLECTION_FILE, sep='\t')

    # Ensure the correct data types for the 'body' and 'publish_datetime' columns
    if 'body' in article_list_df.columns:
        article_list_df['body'] = article_list_df['body'].astype('object')  # Ensure 'body' is of type string (object)

    # No need to convert 'publish_datetime' since it's already in the correct format
    if 'publish_datetime' in article_list_df.columns:
        article_list_df['publish_datetime'] = article_list_df['publish_datetime'].astype('object')  # Ensure 'publish_datetime' is treated as string

    # Keep records where 'publish_datetime' or 'body' or both are missing (NaN)
    articles_to_process = article_list_df[pd.isna(article_list_df['publish_datetime']) | pd.isna(article_list_df['body'])]

    if articles_to_process.empty:
        logging.info("No new articles to process.")
        return

    # Process each article
    for _, row in articles_to_process.iterrows():
        url = row['link']
        html = get_html(url)
        
        if html:
            body, publish_datetime = parse_article_html(html)
            if body and publish_datetime:
                # Update the DataFrame with the new values
                article_list_df.at[row.name, 'body'] = str(body)  # Ensure 'body' is explicitly converted to string
                article_list_df.at[row.name, 'publish_datetime'] = publish_datetime  # Keep 'publish_datetime' as string            

    # Save the updated article list with the processed information
    article_list_df.to_csv(ARTICLE_COLLECTION_FILE, sep='\t', index=False)
    logging.info(f"Updated {ARTICLE_COLLECTION_FILE} with processed articles.")

if __name__ == '__main__':
    
    # Set up logging
    logger_setup.setup_logging()
    logger = logging.getLogger()

    # Set headers to mimic a browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    get_articles()