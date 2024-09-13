import requests
from bs4 import BeautifulSoup
import re

# Set headers to mimic a browser
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}

urls = [
    'https://www.gate.io/announcements/article/38540',
    'https://www.gate.io/announcements/article/37002',
    'https://www.gate.io/announcements/article/37873',
    'https://www.gate.io/announcements/article/38903',
    'https://www.gate.io/announcements/article/38569',
    'https://www.gate.io/announcements/article/39227',
    'https://www.gate.io/announcements/article/38447'

]

for url in urls:
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Parse 'article-details-box' from the soup
    article_details_box = soup.find('div', class_='article-details-box')
    main_content_div = article_details_box.find('div', class_='article-details-main')
    main_content = main_content_div.get_text(strip=True, separator='\n') if main_content_div else ''
    
    #################################################

    main_content = re.sub(r'\[([^\]]+)\]\(\s*(?:[^\s\)]+)(?:\s+"[^"]*")?\s*\)', r'\1', main_content) # Remove markdown links
    main_content = re.sub(r'【.*?】', '', main_content)  # Remove text within 【 and 】
    main_content = re.sub(r'!\[.*?\]\(.*?\)', '', main_content)  # Remove markdown images
    
    main_content = re.sub(r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F700-\U0001F77F\u2700-\u27BF\uFE0F]', '', main_content)
    main_content = re.sub(r'\u00A0', ' ', main_content)
    
    main_content = re.sub(r'：', ': ', main_content)
    main_content = re.sub(r'\.\.', '.', main_content)
    main_content = re.sub(r' ,', ',', main_content)
    main_content = re.sub(r' :', ':', main_content)

    main_content = re.sub(r'\u2013', '-', main_content)  # Replaces en dash with a hyphen
    main_content = re.sub(r'["“”‘’]', '', main_content)
    main_content = re.sub(r'\t+', ' ', main_content)

    main_content = re.sub(r'&', 'and', main_content)

    main_content = re.sub(r'＆', 'and', main_content)
    main_content = re.sub(r'（', '(', main_content)
    main_content = re.sub(r'）', ')', main_content)
    main_content = re.sub(r'●', '•', main_content)

    main_content = re.sub(r'\n\s+', '\n', main_content)
    main_content = re.sub(r'\n{2,}', '\n', main_content)

    main_content = re.sub(r'\[//\]:content-type-MARKDOWN-DONOT-DELETE\s*\n?', '', main_content)
    main_content = re.sub(r'\s*Gateway to Crypto.*', '', main_content, flags=re.DOTALL).rstrip()

    #main_content = re.sub(r'(?:\r\n|\r|\n|\u2028|\u2029)+', '///', main_content)

    ##################################################

    # Extract the filename from the URL
    filename = url.split('/')[-1] + '.txt'
    
    # Write the 'article-details-box' content to the file
    with open(filename, 'w', encoding='utf-8') as file:
        if article_details_box:
            file.write(str(main_content))
        else:
            file.write('No article-details-box found in the page.')

