#File: gateio_get_json.py

import pandas as pd
import openai
import os
from tqdm import tqdm
import json
import logging
import re  # Import regular expressions to help remove trailing commas
import time

# Function to interact with the LLM for a specific instruction using an existing assistant
def get_llm_response(content, title, max_retries=3, backoff_factor=2):
    retries = 0
    
    if "Bi-Weekly Report" in title:
        # Use the existing assistant ID
        assistant_id = 'asst_vsTB4KYxPD3G8m1Kn6fJnUnS'
    else:
        # Normal assistant
        assistant_id = 'asst_vIbs0DWbJUOxY4yuS0pbTL2q'

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

# Main function to handle the entire process
def get_json():
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

        # Construct the content to be sent to the LLM
        content = f"publish_datetime: {publish_datetime}\narticle_link: {article_link}\n{title}\n{body}"

        # Get the response from the LLM
        response = get_llm_response(content, title)

        # Try to parse the response as JSON
        try:
            # Strip and load the JSON to validate it
            parsed_response = json.loads(response.strip())
            
            # Append the raw and parsed responses to the respective lists
            raw_responses.append(response)
            parsed_responses.append(parsed_response)
            
            # If the response is valid JSON, mark the article as processed
            df.at[index, 'llm_processed'] = 'Yes'

        except json.JSONDecodeError as e:
            # Log the error and the problematic response, but don't mark as processed
            logging.error(f"Failed to parse JSON: {e} - Response: {response}")
            continue  # Move to the next article without marking this one as processed

    # Save the updated DataFrame to the TSV file
    df.to_csv('gateio_article_collection.tsv', sep='\t', index=False)

    # Save the parsed JSON objects as a valid JSON array
    with open('gateio_article_events.json', 'w') as f:
        json.dump(parsed_responses, f, indent=4)

    print("The JSON file has been saved as 'gateio_article_events.json'.")

# Run the main function only when the script is executed directly
if __name__ == "__main__":

    # Set up logging for error tracking
    logging.basicConfig(filename='gateio_errors.log', level=logging.ERROR)

    # Set up OpenAI API key
    os.environ['OPENAI_API_KEY'] = 'sk-proj-vCcWskCtsmbnloDfRC6XT3BlbkFJURTh97LuwNhvGOMqgWgh'
    
    # Initialize OpenAI client (based on the actual SDK you use)
    openai.api_key = os.getenv('OPENAI_API_KEY')
    global client
    client = openai.OpenAI()  # Ensure correct initialization method for the version of OpenAI SDK you're using

    get_json()