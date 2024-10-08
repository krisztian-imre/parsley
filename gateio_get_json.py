#File: gateio_get_json.py

import pandas as pd
import openai
import os
from tqdm import tqdm
import json
import logging
import re  # Import regular expressions to help remove trailing commas
import time

# Set up logging for error tracking
logging.basicConfig(filename='gateio_errors.log', level=logging.ERROR)

# Set up OpenAI API key
os.environ['OPENAI_API_KEY'] = 'sk-proj-vCcWskCtsmbnloDfRC6XT3BlbkFJURTh97LuwNhvGOMqgWgh'

client = openai.OpenAI()

# Use the existing assistant ID
assistant_id = 'asst_vIbs0DWbJUOxY4yuS0pbTL2q'

# Function to interact with the LLM for a specific instruction using an existing assistant
def get_llm_response(content, max_retries=3, backoff_factor=2):
    retries = 0
    
    while retries < max_retries:
        try:
            # Create a new thread for each interaction
            thread = client.beta.threads.create()

            # Send the content as a message
            client.beta.threads.messages.create(
                thread_id=thread.id,
                role="user",
                content=content
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
                        response = message.content[0].text.value
                        response = response.replace('```json', '')  # Remove starting ```json marker
                        response = response.replace('```', '')      # Remove ending ```
                        if response:
                            print(response)
                            return response
            else:
                logging.error(f"Unexpected run status: {run.status}")
                raise Exception("LLM run did not complete successfully")

        except json.JSONDecodeError as e:
            logging.error(f"JSON Decode Error: {e} - Content: {content}")
            return "LLM_ERROR"
        
        except openai.error.RateLimitError as rate_limit_err:
            logging.error(f"Rate Limit Error: {rate_limit_err} - Content: {content}")
            # Backoff and retry for rate-limiting issues
            retries += 1
            sleep_time = backoff_factor ** retries
            logging.warning(f"Rate limit exceeded, retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
            continue

        except openai.error.APIConnectionError as connection_err:
            logging.error(f"API Connection Error: {connection_err} - Content: {content}")
            # Backoff and retry for connection issues
            retries += 1
            sleep_time = backoff_factor ** retries
            logging.warning(f"Connection issue, retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
            continue

        except openai.error.Timeout as timeout_err:
            logging.error(f"Timeout Error: {timeout_err} - Content: {content}")
            # Backoff and retry for timeouts
            retries += 1
            sleep_time = backoff_factor ** retries
            logging.warning(f"Timeout issue, retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
            continue

        except openai.error.OpenAIError as openai_err:
            logging.error(f"OpenAI API Error: {openai_err} - Content: {content}")
            return "LLM_ERROR"

        except Exception as e:
            logging.error(f"Unexpected Error: {e} - Content: {content}")
            return "LLM_ERROR"

    # If all retries fail
    logging.error(f"Failed to process content after {max_retries} retries.")
    return "LLM_ERROR"

# Load the TSV file into a pandas DataFrame
df = pd.read_csv('gateio_article_collection.tsv', sep='\t')

# Filter rows where 'llm_processed' is 'No'
unprocessed_records = df[df['llm_processed'] == 'No']

# Initialize an empty list to store raw responses and parsed responses
raw_responses = []
parsed_responses = []

# Process each unprocessed record
for index, row in tqdm(unprocessed_records.iterrows(), total=len(unprocessed_records)):
    
    title = row['title']
    body = row['body']
    publish_datetime = row['publish_datetime']
    article_link = row['link']

    content = f"publish_datetime: {publish_datetime}\narticle_link: {article_link}\n{title}\n{body}"

    # Get the response from the LLM
    response = get_llm_response(content)
    
    # Append the raw response to the list
    raw_responses.append(response)

    # Update the DataFrame to mark it as processed (commented in your code)
    # df.at[index, 'llm_processed'] = 'Yes'

# Save the updated DataFrame back to the TSV file (uncomment if needed)
# df.to_csv('gateio_articles.tsv', sep='\t', index=False)

# Iterate over each string in the raw data and parse it
for json_string in raw_responses:
    try:
        # Strip any surrounding whitespace and parse each JSON string
        parsed_response = json.loads(json_string.strip())
        parsed_responses.append(parsed_response)
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse JSON: {e}")
        logging.error(f"Problematic JSON: {json_string}")

# Save the parsed JSON objects as a valid JSON array
with open('gateio_json.json', 'w') as f:
    json.dump(parsed_responses, f, indent=4)

print("The JSON file has been fixed and saved as 'gateio_json.json'.")
