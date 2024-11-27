#File: gateio_get_article_list_new.py

import os
import pandas as pd
import time
import random
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime, timezone
import re
import logging
import gateio_logger_setup

ARTICLE_COLLECTION_FILE = os.path.expanduser('~/parsley/Gateio_Files/Gateio_Article_Process/gateio_article_collection.tsv')
ARTICLE_CATEGORIES_FILE = os.path.expanduser('~/parsley/gateio_categories.tsv')

# Function to load URLs and categories from the txt file
def load_gateio_categories(filename = ARTICLE_CATEGORIES_FILE):
    gateio_categories = {}
    with open(filename, 'r') as file:
        for line in file:
            url, category = line.strip().split('\t')  # Split by tab
            gateio_categories[url] = category
    return gateio_categories

# Function to clean title
def clean_title(title):
    title = re.sub(r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F700-\U0001F77F\u2700-\u27BF\u2600-\u26FF\uFE0F]', '', title)
    title = re.sub(r'\u00A0', ' ', title)
    title = re.sub(r'[\u25CB-\u25EF\u2B55\u1F9E7\u2B50]', '', title)

    title = re.sub(r'\uff1a', ': ', title)
    title = re.sub(r'\uff01', '! ', title)
    title = re.sub(r'\.\.', '.', title)
    title = re.sub(r' ,', ',', title)
    title = re.sub(r' :', ':', title)

    title = re.sub(r'\u2013', '-', title)  # Replaces en dash with a hyphen
    title = re.sub(r'["\u201c\u201d\u2018\u2019]', '', title)
    title = re.sub(r'\t+', ' ', title)
    title = re.sub(r'&', 'and', title)

    title = re.sub(r'\u3010.*?\u3011', '', title)  # Remove text within ［ and ］
    title = re.sub(r'\uff06', 'and', title)
    title = re.sub(r'\uff08', '(', title)
    title = re.sub(r'\uff09', ')', title)
    title = re.sub(r'\u25cf', '\u2022', title)

    return title.strip()

# Function to get HTML content with retry mechanism
def get_html(url, max_retries=3, backoff_factor=2):
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            retries += 1
            wait_time = backoff_factor ** retries  # Exponential backoff
            print(f"Error fetching {url} (retries {retries}/{max_retries}): {e}. Retrying in {wait_time:.2f} seconds.")
            time.sleep(wait_time)
    return None

# Function to parse HTML content and return articles with full URLs
def parse_html(html, category):
    soup = BeautifulSoup(html, 'html.parser')
    article_list_box = soup.find('div', class_='article-list-box')
    if not article_list_box:
        return None

    articles = article_list_box.find_all('div', class_='article-list-item')
    article_data = []
    
    # Get the current time in UTC with a timezone-aware object and format it as 'YYYY-MM-DD HH:MM:SS UTC'
    current_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S') + ' UTC'

    for article in articles:
        title_tag = article.find('a', class_='article-list-item-title')
        if title_tag:
            title = title_tag.find('h3').get_text(strip=True)
            title = clean_title(title)

            # Construct the full link for the article
            partial_link = title_tag['href']
            full_link = urljoin('https://www.gate.io', partial_link)

            # Append the article data, ensuring correct types
            article_data.append({
                'exchange': 'Gate.io',
                'llm_processed': "No",
                'parse_datetime': current_time,  # This will be a string in the correct format for parsing
                'publish_datetime': None,  # Use None instead of 'NaN' to represent missing values
                'link': full_link,
                'category': category,
                'title': title,
                'body': None  # Use None for missing body content
            })

    return article_data

# Function to append only new articles
def append_new_articles(existing_data, new_data):
    existing_links = set(existing_data['link'])
    new_articles = [article for article in new_data if article['link'] not in existing_links]
    return new_articles

# Function to save data to TSV
def save_data(data, filename = ARTICLE_COLLECTION_FILE):
    df = pd.DataFrame(data)
    df.to_csv(filename, sep='\t', index=False)
    print(f"Data saved to {filename}")

# Main function to scrape multiple URLs
def scrape_website(urls_dict, filename = ARTICLE_COLLECTION_FILE):
    # Ensure the folder structure exists
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    all_articles = []
    
    # Load existing links into memory if file exists
    existing_links = set()
    if os.path.exists(filename):
        existing_data = pd.read_csv(filename, sep='\t')
        existing_links = set(existing_data['link'])
    
    for url, category in urls_dict.items():
        print(f"Scraping category: {category}")
        html = get_html(url)
        if html:
            data = parse_html(html, category=category)
            if data:
                # Append only new articles based on links
                new_articles = [article for article in data if article['link'] not in existing_links]
                
                if new_articles:
                    # Update the existing links set with new articles
                    existing_links.update(article['link'] for article in new_articles)
                    all_articles.extend(new_articles)
                else:
                    print(f"No new articles found for {category}.")
            else:
                print(f"No articles found for {category}")
        else:
            print(f"Failed to fetch {category}: {url}")
        time.sleep(random.uniform(1, 1.75))  # Delay to avoid overwhelming the server

    # Save all new articles once at the end
    if all_articles:
        if os.path.exists(filename):
            existing_data = pd.read_csv(filename, sep='\t')
            updated_data = pd.concat([existing_data, pd.DataFrame(all_articles)], ignore_index=True)
            save_data(updated_data, filename=filename)
        else:
            save_data(all_articles, filename=filename)

    return all_articles

# Function to get the article list
def get_article_list(filename):
    # Load the URLs from the file
    gateio_categories = load_gateio_categories()

    # Display the loaded URLs and categories
    print("Loaded URLs and categories:")
    for url, category in gateio_categories.items():
        print(f"{url}: {category}")

    scrape_website(gateio_categories, filename)

if __name__ == '__main__':

    # Set up logging
    gateio_logger_setup.setup_logging()
    logger = logging.getLogger()

    # Set headers to mimic a browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    get_article_list(ARTICLE_COLLECTION_FILE)
