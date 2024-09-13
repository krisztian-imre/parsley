# File: gateio_get_articles.py

import os
import pandas as pd
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
import time

# Set headers to mimic a browser
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Function to get HTML content with retry mechanism
def get_html(url, max_retries=3, delay=3):
    attempts = 0
    while attempts < max_retries:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.text
        except requests.exceptions.HTTPError as e:
            if response.status_code == 502:
                print(f"502 Server Error: Bad Gateway for url: {url}. Retrying in {delay} seconds...")
                attempts += 1
                time.sleep(delay)
            else:
                print(f"HTTP Error fetching {url}: {e}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            return None
    print(f"Failed to fetch {url} after {max_retries} attempts.")
    return None

# Function to clean main_content (body) content using regex
def clean_body(main_content):
    
    main_content = re.sub(r'\[([^\]]+)\]\((?:[^\s\)]+)(?:\s+"[^"]+")?\)', r'\1', main_content)  # Remove markdown links
    main_content = re.sub(r'!\[.*?\]\(.*?\)', '', main_content)  # Remove markdown images
    
    main_content = re.sub(r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F700-\U0001F77F\u2700-\u27BF\uFE0F]', '', main_content)

    main_content = re.sub(r'：', ': ', main_content)
    main_content = re.sub(r'\.\.', '.', main_content)
    main_content = re.sub(r' ,', ',', main_content)
    main_content = re.sub(r' :', ':', main_content)

    main_content = re.sub(r'\u2013', '-', main_content)  # Replaces en dash with a hyphen
    main_content = re.sub(r'["“”‘’]', '', main_content)
    main_content = re.sub(r'\t+', ' ', main_content)
    main_content = re.sub(r'&', 'and', main_content)

    main_content = re.sub(r'【.*?】', '', main_content)  # Remove text within 【 and 】
    main_content = re.sub(r'＆', 'and', main_content)
    main_content = re.sub(r'（', '(', main_content)
    main_content = re.sub(r'）', ')', main_content)

    main_content = re.sub(r'\n\s+', '\n', main_content)
    main_content = re.sub(r'\n{2,}', '\n', main_content)

    main_content = re.sub(r'\[//\]:content-type-MARKDOWN-DONOT-DELETE\s*\n?', '', main_content)
    main_content = re.sub(r'\s*Gateway to Crypto.*', '', main_content, flags=re.DOTALL).rstrip()

    main_content = re.sub(r'(?:\r\n|\r|\n|\u2028|\u2029)+', '///', main_content)

    return main_content #body

# Function to parse article HTML content
def parse_article_html(html):
    soup = BeautifulSoup(html, 'html.parser')

    # Extract publish time
    article_details_box = soup.find('div', class_='article-details-box')
    if not article_details_box:
        return None, None
    
    publish_time = article_details_box.find('div', class_='article-details-base-info').find_all('span')[0].get_text(strip=True)
    publish_time = re.sub(r' UTC$', '', publish_time)

    # Extract the main content within the article-details-main
    main_content_div = article_details_box.find('div', class_='article-details-main')
    main_content = main_content_div.get_text(strip=True, separator='\n') if main_content_div else ''

    # Clean the content using the new clean_body function
    main_content = clean_body(main_content)
    
    return main_content, publish_time

# Function to process articles from gateio_article_list.tsv
def process_articles(article_list_file='gateio_article_list.tsv', articles_file='gateio_articles.tsv'):
    # Load the article list
    article_list_df = pd.read_csv(article_list_file, sep='\t')

    # Filter out records where 'processed' == 'No'
    articles_to_process = article_list_df[article_list_df['processed'] == 'No']

    if articles_to_process.empty:
        print("No new articles to process.")
        return

    # If gateio_articles.tsv exists, load it, otherwise create an empty DataFrame
    if os.path.exists(articles_file):
        existing_articles_df = pd.read_csv(articles_file, sep='\t')
    else:
        existing_articles_df = pd.DataFrame(columns=['exchange', 'link', 'category', 'title', 'publish_time', 'scraping_time', 'llm_processed', 'body'])

    # Initialize a list to hold new article data
    new_articles = []

    # Process each article
    for _, row in tqdm(articles_to_process.iterrows(), total=len(articles_to_process), desc="Processing articles"):
        url = row['link']
        html = get_html(url)
        
        if html:
            body, publish_time = parse_article_html(html)
            if body and publish_time:
                # Create a new article record
                new_article = {
                    'exchange': row['exchange'],
                    'link': row['link'],
                    'category': row['category'],
                    'title': row['title'],
                    'publish_time': publish_time,
                    'scraping_time': row['scraping_time'],
                    'llm_processed': 'No',  # default value
                    'body': body
                }
                new_articles.append(new_article)

                # Mark the article as processed in the original list
                article_list_df.loc[article_list_df['link'] == row['link'], 'processed'] = 'Yes'

        # Add a delay to avoid overwhelming the server
        time.sleep(1)

    # If there are new articles, append them to gateio_articles.tsv
    if new_articles:
        new_articles_df = pd.DataFrame(new_articles)
        updated_articles_df = pd.concat([existing_articles_df, new_articles_df], ignore_index=True)
        updated_articles_df.to_csv(articles_file, sep='\t', index=False)
        print(f"New articles saved to {articles_file}")

    # Save the updated article list with 'processed' updated to 'Yes'
    article_list_df.to_csv(article_list_file, sep='\t', index=False)
    print(f"Updated {article_list_file} with processed status.")

if __name__ == '__main__':
    process_articles()