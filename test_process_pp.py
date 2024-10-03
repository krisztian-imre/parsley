# File: test_process_pp.py

import pandas as pd
import openai
import os
from tqdm import tqdm
from time import sleep
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from openai import OpenAIError  # Importing directly from openai


# Set up OpenAI API key
os.environ['OPENAI_API_KEY'] = 'sk-proj-vCcWskCtsmbnloDfRC6XT3BlbkFJURTh97LuwNhvGOMqgWgh'

client = openai.OpenAI()

# Use the existing assistant ID
assistant_id = 'asst_P5Tzr3qSys9swxffeygqvy6x'

# Load instructions for different tasks
with open('instruction-eventtype-wo-list.txt', 'r') as file:
    instruction_eventtype_wo_list = file.read()

with open('instruction-category.txt', 'r') as file:
    instruction_category = file.read()

with open('instruction-eventtype-actionable.txt', 'r') as file:
    instruction_eventtype_actionable = file.read()

# Define the expected number of features (columns)
expected_feature_count = 5  # Adjust this if the number of expected columns changes

# Function to check if the record has the correct number of features
def record_health_check(record, expected_count):
    actual_count = len(record)
    llm_error_found = any('LLM_ERROR' in str(value) for value in record.values())

    if llm_error_found:
        print("Error: 'LLM_ERROR' found in one of the features.")
        return False
    
    if actual_count == expected_count:
        return True
    else:
        print(f"Error: Record has {actual_count} features, expected {expected_count}.")
        return False

# Retry logic to handle OpenAI rate limits using tenacity
@retry(wait=wait_exponential(multiplier=1, min=4, max=60), stop=stop_after_attempt(6),
       retry=retry_if_exception_type(OpenAIError))  # Using OpenAIError directly
def get_llm_response(content, instruction, retries=3):
    for attempt in range(retries):
        try:
            sleep(1)
            thread = client.beta.threads.create()
            full_message = f"{instruction}\n\n{content}"
            client.beta.threads.messages.create(
                thread_id=thread.id,
                role="user",
                content=full_message
            )
            run = client.beta.threads.runs.create_and_poll(
                thread_id=thread.id,
                assistant_id=assistant_id
            )

            if run.status == "completed":
                messages = client.beta.threads.messages.list(thread_id=thread.id)
                for message in messages:
                    if message.role == "assistant" and hasattr(message, 'content'):
                        response = message.content[0].text.value
                        if response.strip():
                            return response  # Return valid response
        except OpenAIError as e:  # Catching the OpenAIError directly
            print(f"OpenAI API error: {e}")
            if attempt == retries - 1:
                return "LLM_ERROR"  # Return error after max retries
    return "LLM_ERROR"  # Default error after retries

# Function to process a single record using parallel API calls
def process_record(index, row):
    content = row['body']
    title = row['title']

    # Define tasks for parallel execution
    tasks = {
        'llm_category': lambda: get_llm_response(title + '///' + content, instruction_category),
        'llm_eventtype_wo_list': lambda: get_llm_response(title + '///' + content, instruction_eventtype_wo_list),
        'llm_eventtype_actionable': lambda: get_llm_response(title + '///' + content, instruction_eventtype_actionable)
    }

    results = {}

    # Execute tasks in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(func): key for key, func in tasks.items()}
        for future in as_completed(futures):
            key = futures[future]
            results[key] = future.result()

    # Extract results
    llm_category = re.sub(r'^.*?(\[.*?\]).*$', r'\1', results['llm_category'])
    llm_eventtype_wo_list = re.sub(r'^.*?(\[.*?\]).*$', r'\1', results['llm_eventtype_wo_list'])
    llm_eventtype_actionable = re.sub(r'^.*?(\[.*?\]).*$', r'\1', results['llm_eventtype_actionable'])

    # Construct the processed record
    processed_record = {
        'llm_category': llm_category,
        'llm_eventtype_wo_list': llm_eventtype_wo_list,
        'llm_eventtype_actionable': llm_eventtype_actionable,
        'in_category': row['in_category'],
        'link': row['link']
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
with ThreadPoolExecutor(max_workers=5) as executor:  # Limit number of concurrent requests
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

# Save the data to the 'test_processed.tsv' file
output_file = 'test_processed.tsv'
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

