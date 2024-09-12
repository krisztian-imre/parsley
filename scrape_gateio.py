import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import random
import time
from urllib.parse import urljoin  # To handle URLs properly
from datetime import datetime  # For capturing the current scraping time
import re

# Set headers to mimic a browser
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# List of URLs and categories with full URLs
gateio_urls = {
    "https://www.gate.io/announcements/activity": "Activities",
    "https://www.gate.io/announcements/dau": "Bi-Weekly Report",
    "https://www.gate.io/announcements/institutional": "Institutional & VIP",
    "https://www.gate.io/announcements/gate-learn": "Gate Learn",
    "https://www.gate.io/announcements/delisted": "Delisting",
    "https://www.gate.io/announcements/wealth": "Gate Wealth",
    "https://www.gate.io/announcements/newlisted": "New Cryptocurrency Listings",
    "https://www.gate.io/announcements/charity": "Gate Charity",
    "https://www.gate.io/announcements/finance": "Finance",
    "https://www.gate.io/announcements/trade-match": "Trading Competitions",
    "https://www.gate.io/announcements/deposit-withdrawal": "Deposit & Withdrawal",
    "https://www.gate.io/announcements/etf": "ETF",
    "https://www.gate.io/announcements/fee": "Fee",
    "https://www.gate.io/announcements/lives": "Live",
    "https://www.gate.io/announcements/gatecard": "Gate Card",
    "https://www.gate.io/announcements/rename": "Token Rename",
    "https://www.gate.io/announcements/engine-upgrade": "Engine Upgrade",
    "https://www.gate.io/announcements/fiat": "Fiat",
    "https://www.gate.io/announcements/precision": "Precision",
    "https://www.gate.io/announcements/p2p": "P2P Trading"
}

# Function to clean the title
def clean_title(title):
    # Remove all quotation marks
    title = re.sub(r'["“”‘’]', '', title)
    
    # Remove tabs
    title = re.sub(r'\t+', ' ', title)
    
    # Replace double or more spaces with a single space
    title = re.sub(r'\s{2,}', ' ', title)
    
    # Add space before '(' if no space before it (e.g., 'a(' becomes 'a (')
    title = re.sub(r'(\S)\(', r'\1 (', title)
    
    # Add space after ')' if no space after it (e.g., ')b' becomes ') b')
    title = re.sub(r'\)(\S)', r') \1', title)
    
    return title.strip()  # Ensure no leading or trailing spaces

# Function to get HTML content
def get_html(url):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None

# Function to parse HTML content and return articles with full URLs
def parse_html(html, category):
    soup = BeautifulSoup(html, 'html.parser')

    # Extract the relevant parts using specific classes
    article_list_box = soup.find('div', class_='article-list-box')
    if not article_list_box:
        return None

    # Extract individual articles
    articles = article_list_box.find_all('div', class_='article-list-item')
    article_data = []

    # Capture the current time for each scraping action
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for article in articles:
        title_tag = article.find('a', class_='article-list-item-title')
        if title_tag:
            # Extract the title from the <span> inside the <h3>
            title = title_tag.find('h3').get_text(strip=True)

            # Clean the title using the new clean_title function
            title = clean_title(title)

            # Extract the partial link and create the full link using urljoin
            partial_link = title_tag['href']
            full_link = urljoin('https://www.gate.io', partial_link)

            # Add scraping time and set processed to 'No'
            article_data.append({
                'title': title,
                'link': full_link,
                'category': category,
                'scraping_time': current_time,  # Use current scraping time
                'processed': 'No'  # Default value for processed
            })

    return article_data

# Function to save data to TSV
def save_data(data, filename='gateio_article_list.tsv'):
    df = pd.DataFrame(data)
    df.to_csv(filename, sep='\t', index=False)  # Use tab-separated format
    print(f"Data saved to {filename}")

# Main function to scrape multiple URLs
def scrape_website(urls_dict):
    all_articles = []
    for url, category in tqdm(urls_dict.items(), desc="Scraping categories"):
        print(f"Scraping {category}: {url}")  # Log the current URL being scraped
        html = get_html(url)
        if html:
            data = parse_html(html, category=category)
            if data:
                all_articles.extend(data)
            else:
                print(f"No articles found for {category}")
        else:
            print(f"Failed to fetch {category}: {url}")
        time.sleep(random.uniform(1, 1.75))  # Add a delay to avoid overwhelming the server (Bad Gateway Error)
    return all_articles

# Start scraping
scraped_data = scrape_website(gateio_urls)

# Save the scraped data
if scraped_data:
    save_data(scraped_data)

# Display the first 5 records of the scraped data
df_scraped_data = pd.DataFrame(scraped_data)
print(df_scraped_data[['title', 'link', 'category', 'scraping_time', 'processed']].head())