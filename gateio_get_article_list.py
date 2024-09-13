# File: gateio_get_article_list.py

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

# Check if file exists
filename = 'gateio_article_list.tsv'

# Set headers to mimic a browser
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Function to load URLs and categories from the txt file
def load_gateio_urls(filename='gateio_urls.txt'):
    gateio_urls = {}
    with open(filename, 'r') as file:
        for line in file:
            url, category = line.strip().split('\t')  # Split by tab
            gateio_urls[url] = category
    return gateio_urls

# Load the URLs from the file
gateio_urls = load_gateio_urls()

# Display the loaded URLs and categories
print("Loaded URLs and categories:")
for url, category in gateio_urls.items():
    print(f"{url}: {category}")

# Function to clean title
def clean_title(title):
    title = re.sub(r'["“”‘’]', '', title)
    title = re.sub(r'\t+', ' ', title)
    title = title.replace('：', ': ').replace('..', '.').replace(' –', '–').replace('– ', '–').replace(' ,', ',').replace(' :', ':')
    title = re.sub(r'\s+!', '!', title)
    title = re.sub(r'\s{2,}', ' ', title)
    title = re.sub(r'(\S)\(', r'\1 (', title)
    title = re.sub(r'\)(\S)', r') \1', title)
    title = re.sub(r'\(\s+', '(', title)
    title = re.sub(r'\s+\)', ')', title)
    return title.strip()

# Function to get HTML content with retry mechanism
def get_html(url, max_retries=5, backoff_factor=1):
    attempt = 0
    while attempt < max_retries:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            attempt += 1
            wait_time = backoff_factor * (2 ** (attempt - 1))  # Exponential backoff
            print(f"Error fetching {url} (attempt {attempt}/{max_retries}): {e}. Retrying in {wait_time:.2f} seconds.")
            time.sleep(wait_time)

# Function to parse HTML content and return articles with full URLs
def parse_html(html, category):
    soup = BeautifulSoup(html, 'html.parser')
    article_list_box = soup.find('div', class_='article-list-box')
    if not article_list_box:
        return None
    articles = article_list_box.find_all('div', class_='article-list-item')
    article_data = []
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for article in articles:
        title_tag = article.find('a', class_='article-list-item-title')
        if title_tag:
            title = title_tag.find('h3').get_text(strip=True)
            title = clean_title(title)
            partial_link = title_tag['href']
            full_link = urljoin('https://www.gate.io', partial_link)
            article_data.append({
                'exchange': 'Gate.io',
                'link': full_link,
                'category': category,
                'title': title,
                'scraping_time': current_time,
                'processed': 'No'
            })
    return article_data

# Function to append only new articles
def append_new_articles(existing_data, new_data):
    existing_links = set(existing_data['link'])
    new_articles = [article for article in new_data if article['link'] not in existing_links]
    return new_articles

# Function to save data to TSV
def save_data(data, filename='gateio_article_list.tsv'):
    df = pd.DataFrame(data)
    df.to_csv(filename, sep='\t', index=False)
    print(f"Data saved to {filename}")

# Main function to scrape multiple URLs
def scrape_website(urls_dict):
    all_articles = []
    for url, category in tqdm(urls_dict.items(), desc="Scraping categories", ncols = 100):
        print(f"Scraping {category}: {url}")
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

# Check if file exists
if os.path.exists(filename):
    # Load the existing data
    existing_data = pd.read_csv(filename, sep='\t')
    # Scrape new data
    new_data = scrape_website(gateio_urls)
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
    scraped_data = scrape_website(gateio_urls)
    if scraped_data:
        save_data(scraped_data, filename=filename)
    else:
        print("No data scraped.")
