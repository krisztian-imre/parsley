# File: gateio_get_jsons.py

import os
import re
import json
import openai
import pandas as pd
from datetime import datetime

# Set your OpenAI API key here
openai.api_key = 'sk-proj-vCcWskCtsmbnloDfRC6XT3BlbkFJURTh97LuwNhvGOMqgWgh'

# File paths
DATA_FILE = 'gateio_articles.tsv'
JSON_FOLDER = 'Unprocessed_JSON'

# Ensure the output folder exists
if not os.path.exists(JSON_FOLDER):
    os.makedirs(JSON_FOLDER)

# Load data
df = pd.read_csv(DATA_FILE, sep='\t')

# Initialize ID counter
id_counter = 1

# Function to convert '///' to '\n'
def clean_body_text(body):
    return re.sub(r'///', '\n', body)

# Function to send text to OpenAI API
def call_openai_api(prompt):
    response = openai.Completion.create(
        model="gpt-4", # You can choose a different model
        prompt=prompt,
        max_tokens=500, # Adjust as needed
        n=1,
        stop=None,
        temperature=0.7
    )
    return response.choices[0].text.strip()

# Function to process each article
def process_article(row, id_counter):
    # Clean the body text
    clean_body = clean_body_text(row['body'])
    
    # Prepare prompt for the LLM
    prompt = f"""
    Extract the following structured information from the event description:
    - Event type (confirm if it is delisting, listing, or anything else)
    - Event date (any dates mentioned)
    - Coin (extract coin symbols like BTC, ETH, etc.)
    - Market (e.g., BTC/USDT, XYZ/BNB)
    - Market type (spot, perpetual, etc.)
    - Priority (high, medium, low)

    Event description:
    {clean_body}
    """

    # Call OpenAI API to process
    structured_output = call_openai_api(prompt)

    # Create structured output
    output_data = {
        'id': f'{id_counter:08d}',  # Format ID as 8-digit string
        'link': row['link'],
        'parse_datetime': row['parse_datetime'],
        'exchange': row['exchange'],
        'publish_datetime': row['publish_datetime'],
        'event_type': row['in_category'],  # LLM should validate
        'structured_output': structured_output  # Assuming this has multiple event dates, coins, etc.
    }

    return output_data

# Process all articles
for index, row in df.iterrows():
    # Skip if already processed
    if row['llm_processed'] == 'Yes':
        continue
    
    # Process the article
    structured_data = process_article(row, id_counter)
    
    # Save the structured data to a JSON file
    json_file_path = os.path.join(JSON_FOLDER, f'{structured_data["id"]}.json')
    with open(json_file_path, 'w') as json_file:
        json.dump(structured_data, json_file, indent=4)

    # Update the 'llm_processed' status in the DataFrame
    df.at[index, 'llm_processed'] = 'Yes'

    # Increment ID counter
    id_counter += 1

# Save the updated DataFrame back to the TSV file
df.to_csv(DATA_FILE, sep='\t', index=False)
