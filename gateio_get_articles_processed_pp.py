# File: gateio_get_articles_processed_pp.py

import pandas as pd
import openai
import os
from tqdm import tqdm  # Import tqdm for progress bar
from time import sleep
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set up OpenAI API key
os.environ['OPENAI_API_KEY'] = 'sk-proj-vCcWskCtsmbnloDfRC6XT3BlbkFJURTh97LuwNhvGOMqgWgh'

client = openai.OpenAI()

# Use the existing assistant ID
assistant_id = 'asst_QArsWr5MGsrqgy7UJraQlIMW'

# Load instructions for different tasks
with open('instruction-summary.txt', 'r') as file:
    instruction_summary = file.read()

with open('instruction-category.txt', 'r') as file:
    instruction_category = file.read()

with open('instruction-token.txt', 'r') as file:
    instruction_token = file.read()

with open('instruction-pair.txt', 'r') as file:
    instruction_pair = file.read()

with open('instruction-trading-contract.txt', 'r') as file:
    instruction_trading_contract = file.read()

with open('instruction-earning-product.txt', 'r') as file:
    instruction_earning_product = file.read()

with open('instruction-action-datetime.txt', 'r') as file:
    base_instruction_action_datetime = file.read()  # Load the base template

with open('instruction-priority.txt', 'r') as file:
    instruction_priority = file.read()

# Define the expected number of features (columns)
expected_feature_count = 16  # Adjust this if the number of expected columns changes

# Function to check if the record has the correct number of features
def record_health_check(record, expected_count):
    # Count the number of keys (features) in the record
    actual_count = len(record)
    
    # Check if any feature contains the string 'LLM_ERROR'
    llm_error_found = any('LLM_ERROR' in str(value) for value in record.values())

    if llm_error_found:
        print("Error: 'LLM_ERROR' found in one of the features.")
        return False
    
    # Check if the number of features matches the expected count
    if actual_count == expected_count:
        return True
    else:
        print(f"Error: Record has {actual_count} features, expected {expected_count}.")
        return False

# Function to interact with the LLM for a specific instruction using an existing assistant
def get_llm_response(content, instruction, retries=3):
    for attempt in range(retries):

        sleep(1)

        # Create a new thread for each interaction
        thread = client.beta.threads.create()

        # Combine the content and instruction to send it as a single user message
        full_message = f"{instruction}\n\n{content}"

        # Send the content as a message
        client.beta.threads.messages.create(
            thread_id=thread.id,
            role="user",
            content=full_message
        )
    
        # Poll the LLM with the existing assistant
        run = client.beta.threads.runs.create_and_poll(
            thread_id=thread.id,
            assistant_id=assistant_id
        )

        # Check the run status
        if run.status == "completed":
            messages = client.beta.threads.messages.list(thread_id=thread.id)
            for message in messages:
                if message.role == "assistant" and hasattr(message, 'content'):
                    response = message.content[0].text.value  # Extract the response     
                    print(response)
                    # Check if the response is empty, and retry if it is
                    if response == "" or '\n' in response:
                        continue  # Continue to the next attempt if the response is empty
                    else:
                        return response  # Return the valid response
                

    return "LLM_ERROR"

# Function to process a single record using parallel API calls
def process_record(index, row):
    content = row['body']
    title = row['title']
    
    instruction_action_datetime = base_instruction_action_datetime.format(publish_datetime=row['publish_datetime'])
    
    # Define tasks for parallel execution
    tasks = {
        'llm_summary': lambda: get_llm_response(title + '///' + content, instruction_summary),
        'llm_category': lambda: get_llm_response(title + '///' + content, instruction_category),
        'llm_token': lambda: get_llm_response(title + '///' + content, instruction_token),
        'llm_pair': lambda: get_llm_response(title + '///' + content, instruction_pair),
        'llm_trading_contract': lambda: get_llm_response(title + '///' + content, instruction_trading_contract),
        'llm_earning_product': lambda: get_llm_response(title + '///' + content, instruction_earning_product),
        'llm_action_datetime': lambda: get_llm_response(content, instruction_action_datetime)
    }

    results = {}

    # Execute tasks in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(func): key for key, func in tasks.items()}
        for future in as_completed(futures):
            key = futures[future]
            results[key] = future.result()

    # Extract results
    llm_summary = results['llm_summary']
    llm_category = re.sub(r'^.*?(\[.*?\]).*$', r'\1', results['llm_category'])
    llm_token = re.sub(r'^.*?(\[.*?\]).*$', r'\1', results['llm_token'])
    llm_pair = re.sub(r'^.*?(\[.*?\]).*$', r'\1', results['llm_pair'])
    llm_trading_contract = re.sub(r'^.*?(\[.*?\]).*$', r'\1', results['llm_trading_contract'])
    llm_earning_product = re.sub(r'^.*?(\[.*?\]).*$', r'\1', results['llm_earning_product'])

    llm_action_datetime = results['llm_action_datetime']
    if 'Unknown Date' in llm_action_datetime:
        llm_action_datetime = row['publish_datetime']  # Fallback to publish_datetime if LLM fails

    # Sequential execution for dependent task (llm_priority)
    llm_priority = get_llm_response(llm_category + "///" + llm_summary, instruction_priority)

    # Construct the processed record
    processed_record = {
        'ical_processed': 'No',
        'exchange': row['exchange'],
        'llm_category': llm_category,
        'llm_priority': llm_priority,
        'llm_summary': llm_summary,
        'publish_datetime': row['publish_datetime'],
        'llm_action_datetime': llm_action_datetime,
        'llm_token': llm_token,
        'llm_pair': llm_pair,
        'llm_trading_contract': llm_trading_contract,
        'llm_earning_product': llm_earning_product,
        'link': row['link'],
        'parse_datetime': row['parse_datetime'],
        'in_category': row['in_category'],
        'title': row['title'],
        'body': row['body']
    }

    # Validate the processed record
    if record_health_check(processed_record, expected_feature_count):
        return processed_record, index
    else:
        print(f"Record at index {index} failed feature health check.")
        return None, index

# Load the TSV file
data = pd.read_csv('gateio_articles.tsv', sep='\t')

# Filter the rows where 'llm_processed' is 'No'
unprocessed_data = data[data['llm_processed'] == 'No']

# Initialize a list to store processed records
processed_data = []

# Use ThreadPoolExecutor for processing records concurrently
with ThreadPoolExecutor() as executor:
    future_to_index = {executor.submit(process_record, index, row): index for index, row in unprocessed_data.iterrows()}

    for future in tqdm(as_completed(future_to_index), total=unprocessed_data.shape[0], desc="Processing records"):
        processed_record, index = future.result()

        if processed_record is not None:
            # Append processed record
            processed_data.append(processed_record)
            # Mark as processed
            data.at[index, 'llm_processed'] = 'Yes'

# Convert the processed records into a DataFrame
processed_df = pd.DataFrame(processed_data)

# Save the data to the 'gateio_articles_processed.tsv' file
output_file = 'gateio_articles_processed.tsv'
file_exists = os.path.isfile(output_file)

if not file_exists:
    # If the file does not exist, create it with header
    processed_df.to_csv(output_file, sep='\t', index=False, header=True)
else:
    # If the file exists, append to it without header
    processed_df.to_csv(output_file, sep='\t', index=False, mode='a', header=False)

# Save the updated original file with 'llm_processed' marked as 'Yes' for processed records
data.to_csv('gateio_articles.tsv', sep='\t', index=False)

print(f"Data processed and saved to {output_file}")
