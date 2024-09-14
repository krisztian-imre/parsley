# File: gateio_get_jsons.py

import os
import csv
import re
import json
import openai
from datetime import datetime

# Set your OpenAI API key
openai.api_key = 'sk-proj-vCcWskCtsmbnloDfRC6XT3BlbkFJURTh97LuwNhvGOMqgWgh'

# Initialize the OpenAI client
client = openai.OpenAI()

# File paths
TSV_FILE_PATH = 'gateio_articles.tsv'
INSTRUCTIONS_FILE_PATH = 'instructions.txt'
OUTPUT_FOLDER = 'Unprocessed_JSON'

# Ensure the output folder exists
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Load instructions from 'instructions.txt'
with open(INSTRUCTIONS_FILE_PATH, 'r', encoding='utf-8') as f:
    instructions = f.read()

# Read the TSV file
with open(TSV_FILE_PATH, 'r', encoding='utf-8') as tsv_file:
    reader = csv.DictReader(tsv_file, delimiter='\t')
    records = list(reader)

# Create the assistant with the specified instructions
assistant = client.beta.assistants.create(
    name="CryptoExchangeGuru",
    instructions=instructions,
    tools=[{"type": "code_interpreter"}],
    model="gpt-4o-mini",
    temperature=0
)


# Initialize ID counter
current_id = 1

# Process each record
for record in records:
    if record['llm_processed'] == 'No':
        # Replace '///' with '\n' in the 'body' text
        body_text = re.sub(r'///', '\n', record['body'])

        # Prepare the messages for OpenAI API
        messages = [
            {"role": "system", "content": instructions},
            {"role": "user", "content": body_text}
        ]

        # Call the OpenAI API
        try:
            response = openai.ChatCompletion.create(
                model='gpt-4',  # Use 'gpt-3.5-turbo' if you don't have access to 'gpt-4'
                messages=messages,
                temperature=0.0,
            )

            # Extract the assistant's reply
            assistant_reply = response['choices'][0]['message']['content'].strip()

            # Parse the JSON output
            try:
                json_output = json.loads(assistant_reply)
            except json.JSONDecodeError:
                print(f"JSON decoding failed for record with link: {record['link']}")
                continue

            # Assign IDs to each event
            if isinstance(json_output, list):
                for event in json_output:
                    event['id'] = f"{current_id:08d}"
                    current_id += 1
            else:
                json_output['id'] = f"{current_id:08d}"
                current_id += 1

            # Save the JSON output to the 'Unprocessed_JSON' folder
            output_filename = os.path.join(
                OUTPUT_FOLDER, f"{record['exchange']}_{current_id - 1}.json"
            )
            with open(output_filename, 'w', encoding='utf-8') as json_file:
                json.dump(json_output, json_file, ensure_ascii=False, indent=4)

            # Update 'llm_processed' to 'Yes'
            record['llm_processed'] = 'Yes'

        except openai.OpenAIError as e:
            print(f"OpenAI API error for record with link: {record['link']}")
            print(e)
            continue

# Write the updated records back to the TSV file
with open(TSV_FILE_PATH, 'w', encoding='utf-8', newline='') as tsv_file:
    fieldnames = records[0].keys()
    writer = csv.DictWriter(tsv_file, fieldnames=fieldnames, delimiter='\t')
    writer.writeheader()
    writer.writerows(records)

print("Processing complete.")
