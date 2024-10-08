# File: gateio_get_article_list_2.py

import os
import pandas as pd
from tqdm import tqdm
import time
import random
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import re

# Set headers to mimic a browser
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Function to load URLs and categories from the txt file
def load_gateio_categories(filename='gateio_categories.txt'):
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

    title = re.sub(r'：', ': ', title)
    title = re.sub(r'！', '! ', title)
    title = re.sub(r'\.\.', '.', title)
    title = re.sub(r' ,', ',', title)
    title = re.sub(r' :', ':', title)

    title = re.sub(r'\u2013', '-', title)  # Replaces en dash with a hyphen
    title = re.sub(r'["“”‘’]', '', title)
    title = re.sub(r'\t+', ' ', title)
    title = re.sub(r'&', 'and', title)

    title = re.sub(r'【.*?】', '', title)  # Remove text within 【 and 】
    title = re.sub(r'＆', 'and', title)
    title = re.sub(r'（', '(', title)
    title = re.sub(r'）', ')', title)
    title = re.sub(r'●', '•', title)

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

# Function to parse HTML content and return articles with full URLs
def parse_html(html, category):
    soup = BeautifulSoup(html, 'html.parser')
    article_list_box = soup.find('div', class_='article-list-box')
    if not article_list_box:
        return None

    articles = article_list_box.find_all('div', class_='article-list-item')
    article_data = []
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Current time for 'parse_datetime'

    for article in articles:
        title_tag = article.find('a', class_='article-list-item-title')
        if title_tag:
            title = title_tag.find('h3').get_text(strip=True)
            title = clean_title(title)  # Assuming you have a clean_title() function for processing titles

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
def save_data(data, filename='gateio_article_collection.tsv'):
    df = pd.DataFrame(data)
    df.to_csv(filename, sep='\t', index=False)
    print(f"Data saved to {filename}")

# Main function to scrape multiple URLs
def scrape_website(urls_dict):
    all_articles = []
    with tqdm(urls_dict.items(), desc="Scraping categories", ncols=100, leave=False) as progress_bar:
        for url, category in progress_bar:
            progress_bar.set_postfix_str(f"Scraping {category}")
            html = get_html(url)
            if html:
                data = parse_html(html, category=category)
                if data:
                    all_articles.extend(data)
                else:
                    print(f"No articles found for {category}")
            else:
                print(f"Failed to fetch {category}: {url}")
            time.sleep(random.uniform(1, 1.75))  # Delay to avoid overwhelming the server
    return all_articles

def get_article_list(filename='gateio_article_collection.tsv'):

    # Load the URLs from the file
    gateio_categories = load_gateio_categories()

    # Display the loaded URLs and categories
    print("Loaded URLs and categories:")
    for url, category in gateio_categories.items():
        print(f"{url}: {category}")

    # Check if file exists
    if os.path.exists(filename):
        # Load the existing data
        existing_data = pd.read_csv(filename, sep='\t')
        # Scrape new data
        new_data = scrape_website(gateio_categories)
        # Append only new articles
        new_articles = append_new_articles(existing_data, new_data)
        if new_articles:
            # Convert to DataFrame and append new articles
            new_df = pd.DataFrame(new_articles)
            updated_data = pd.concat([existing_data, new_df], ignore_index=True)
            save_data(updated_data, filename=filename)
        else:
            print("No new articles found.")
    else:
        # No file exists, create a new file with scraped data
        scraped_data = scrape_website(gateio_categories)
        if scraped_data:
            save_data(scraped_data, filename=filename)
        else:
            print("No data scraped.")

if __name__ == '__main__':
    get_article_list()
